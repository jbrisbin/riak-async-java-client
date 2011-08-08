/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.jbrisbin.riak.async;

import static com.google.protobuf.ByteString.*;
import static com.jbrisbin.riak.pbc.RiakMessageCodes.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.bucket.BucketProperties;
import com.basho.riak.client.builders.BucketPropertiesBuilder;
import com.basho.riak.client.query.MapReduceResult;
import com.basho.riak.client.query.WalkResult;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.basho.riak.client.raw.query.LinkWalkSpec;
import com.basho.riak.client.raw.query.MapReduceSpec;
import com.basho.riak.client.raw.query.MapReduceTimeoutException;
import com.basho.riak.client.util.CharsetUtils;
import com.basho.riak.pbc.RPB;
import com.google.protobuf.ByteString;
import com.jbrisbin.riak.async.raw.RawAsyncClient;
import com.jbrisbin.riak.async.raw.ServerInfo;
import com.jbrisbin.riak.async.util.DelegatingErrorHandler;
import com.jbrisbin.riak.pbc.RiakObject;
import com.jbrisbin.riak.pbc.RpbFilter;
import com.jbrisbin.riak.pbc.RpbMessage;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.EmptyCompletionHandler;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.memory.HeapMemoryManager;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.WorkerThreadIOStrategy;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RiakAsyncClient implements RawAsyncClient {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private String host = "localhost";
	private Integer port = 8087;
	private Long timeout = 30L;
	private DelegatingErrorHandler errorHandler = new DelegatingErrorHandler();
	private ExecutorService workerPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
	private HeapMemoryManager heap = new HeapMemoryManager();
	private TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
	private FilterChain filterChain;
	private Connection connection;
	private int maxConnectionRetries = 5;
	private AtomicInteger retries = new AtomicInteger(0);
	private LinkedBlockingQueue<RpbRequest> pendingRequests = new LinkedBlockingQueue<RpbRequest>();
	private ObjectMapper mapper = new ObjectMapper();

	public RiakAsyncClient() {
		start();
	}

	public RiakAsyncClient(String host, Integer port) {
		this.host = host;
		this.port = port;
		start();
	}

	public RiakAsyncClient(String host, Integer port, Long timeout) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}

	public RiakAsyncClient registerErrorHandler(Class<? extends Throwable> t, ErrorHandler errorHandler) {
		this.errorHandler.registerErrorHandler(t, errorHandler);
		return this;
	}

	public String getHost() {
		return host;
	}

	public RiakAsyncClient setHost(String host) {
		this.host = host;
		return this;
	}

	public Integer getPort() {
		return port;
	}

	public RiakAsyncClient setPort(Integer port) {
		this.port = port;
		return this;
	}

	public Long getTimeout() {
		return timeout;
	}

	public RiakAsyncClient setTimeout(Long timeout) {
		this.timeout = timeout;
		return this;
	}

	public ExecutorService getWorkerPool() {
		return workerPool;
	}

	public RiakAsyncClient setWorkerPool(ExecutorService workerPool) {
		this.workerPool = workerPool;
		return this;
	}

	public RiakAsyncClient setErrorHandler(DelegatingErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
		return this;
	}

	public DelegatingErrorHandler getErrorHandler() {
		return errorHandler;
	}

	@SuppressWarnings({"unchecked"})
	public void close() {
		try {
			connection.close(new EmptyCompletionHandler<Connection>() {
				@Override public void failed(Throwable throwable) {
					errorHandler.handleError(throwable);
				}

				@Override public void completed(Connection result) {
					try {
						transport.stop();
					} catch (IOException e) {
						failed(e);
					}
					if (log.isDebugEnabled())
						log.debug(String.format("Disconnected from %s:%s", host, port));
				}
			});
		} catch (IOException e) {
			errorHandler.handleError(e);
		}
	}

	private void start() {
		FilterChainBuilder clientChainBuilder = FilterChainBuilder.stateless();
		clientChainBuilder.add(new TransportFilter());
		clientChainBuilder.add(new RpbFilter(heap));
		clientChainBuilder.add(new PendingRequestFilter());
		filterChain = clientChainBuilder.build();

		transport.setKeepAlive(true);
		transport.setTcpNoDelay(true);
		ThreadPoolConfig config = ThreadPoolConfig
				.defaultConfig()
				.setPoolName("riak-async-client")
				.setCorePoolSize(PROCESSORS)
				.setMaxPoolSize(PROCESSORS)
				.setKeepAliveTime(timeout, TimeUnit.SECONDS);
		transport.setWorkerThreadPoolConfig(config);
		transport.setIOStrategy(WorkerThreadIOStrategy.getInstance());
		transport.setProcessor(filterChain);
		try {
			transport.start();
			connection = getConnection();
		} catch (IOException e) {
			errorHandler.handleError(e);
		} catch (InterruptedException e) {
			errorHandler.handleError(e);
		} catch (ExecutionException e) {
			errorHandler.handleError(e);
		} catch (TimeoutException e) {
			errorHandler.handleError(e);
		}
	}

	private Connection getConnection() throws IOException, ExecutionException, TimeoutException, InterruptedException {
		if (null == connection || !connection.isOpen()) {
			if (retries.get() < maxConnectionRetries) {
				connection = transport.connect(new InetSocketAddress(host, port), new EmptyCompletionHandler<Connection>() {
					@Override public void failed(Throwable throwable) {
						errorHandler.handleError(throwable);
						retries.incrementAndGet();
					}

					@Override public void completed(Connection result) {
						if (retries.get() > 0)
							retries.decrementAndGet();
					}
				}).get(timeout, TimeUnit.SECONDS);
				if (log.isDebugEnabled())
					log.debug(String.format("Connected to %s:%s", host, port));
				generateAndSetClientId().get(timeout, TimeUnit.SECONDS);
			}
		}
		return connection;
	}

	@Override public Promise<RiakResponse> fetch(String bucket, String key) throws IOException {
		return fetch(bucket, key, -1);
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<RiakResponse> fetch(String bucket, String key, int readQuorum) throws IOException {
		RPB.RpbGetReq.Builder b = RPB.RpbGetReq.newBuilder()
																					 .setBucket(copyFromUtf8(bucket))
																					 .setKey(copyFromUtf8(key));
		if (readQuorum > 0) {
			b.setR(readQuorum);
		}
		RpbMessage<RPB.RpbGetReq> msg = new RpbMessage<RPB.RpbGetReq>(MSG_GetReq, b.build());
		final Promise<RiakResponse> promise = new Promise<RiakResponse>();
		try {
			getConnection().write(new RpbRequest(msg, promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public Promise<RiakResponse> store(final IRiakObject object, final StoreMeta storeMeta) {
		RPB.RpbPutReq.Builder b = RPB.RpbPutReq.newBuilder()
																					 .setBucket(copyFromUtf8(object.getBucket()))
																					 .setKey(copyFromUtf8(object.getKey()));
		if (null != object.getVClock())
			b.setVclock(copyFrom(object.getVClock().getBytes()));
		if (object instanceof RiakObject) {
			b.setContent(((RiakObject) object).build());
		}
		if (null != storeMeta) {
			if (null != storeMeta.getReturnBody())
				b.setReturnBody(storeMeta.getReturnBody());

			if (null != storeMeta.getDw())
				b.setDw(storeMeta.getDw());

			if (null != storeMeta.getW())
				b.setW(storeMeta.getW());
		}
		RpbMessage<RPB.RpbPutReq> msg = new RpbMessage<RPB.RpbPutReq>(MSG_PutReq, b.build());
		final Promise<RiakResponse> promise = new Promise<RiakResponse>();
		try {
			getConnection().write(new RpbRequest(msg, promise), new EmptyCompletionHandler() {
				@Override public void failed(Throwable throwable) {
					retries.incrementAndGet();
					if (retries.get() < maxConnectionRetries) {
						if (log.isDebugEnabled())
							log.debug("Retrying request: " + object);
						store(object, storeMeta);
					}
				}
			});
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@Override public Promise<RiakResponse> store(IRiakObject object) throws IOException {
		return store(object, null);
	}

	@Override public Promise<Void> delete(String bucket, String key) throws IOException {
		return delete(bucket, key, -1);
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<Void> delete(String bucket, String key, int deleteQuorum) {
		RPB.RpbDelReq.Builder b = RPB.RpbDelReq.newBuilder()
																					 .setBucket(copyFromUtf8(bucket))
																					 .setKey(copyFromUtf8(key));
		if (deleteQuorum > -1)
			b.setRw(deleteQuorum);
		RpbMessage<RPB.RpbDelReq> msg = new RpbMessage<RPB.RpbDelReq>(MSG_DelReq, b.build());
		final Promise<Void> promise = new Promise<Void>();
		try {
			getConnection().write(new RpbRequest(msg, promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<Set<String>> listBuckets() {
		Promise<Set<String>> promise = new Promise<Set<String>>();
		try {
			connection.write(new RpbRequest(new RpbMessage(MSG_ListBucketsReq, null), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<BucketProperties> fetchBucket(String bucketName) {
		RPB.RpbGetBucketReq.Builder b = RPB.RpbGetBucketReq.newBuilder()
																											 .setBucket(copyFromUtf8(bucketName));
		Promise<BucketProperties> promise = new Promise<BucketProperties>();
		try {
			connection.write(new RpbRequest(new RpbMessage(MSG_GetBucketReq, b.build()), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@Override public Promise<Void> updateBucket(String name, BucketProperties bucketProperties) throws IOException {
		throw new UnsupportedOperationException("Updating bucket properties not yet supported");
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<Iterable<String>> listKeys(String bucketName) throws IOException {
		RPB.RpbListKeysReq.Builder b = RPB.RpbListKeysReq.newBuilder()
																										 .setBucket(copyFromUtf8(bucketName));
		Promise<Iterable<String>> promise = new Promise<Iterable<String>>();
		try {
			getConnection().write(new RpbRequest(new RpbMessage(MSG_ListKeysReq, b.build()), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@Override public Promise<WalkResult> linkWalk(LinkWalkSpec linkWalkSpec) throws IOException {
		throw new UnsupportedOperationException("Link walking not yet supported");
	}

	@SuppressWarnings({"unchecked"})
	@Override
	public Promise<MapReduceResult> mapReduce(String json) throws IOException, MapReduceTimeoutException {
		RPB.RpbMapRedReq.Builder b = RPB.RpbMapRedReq.newBuilder()
																								 .setContentType(ByteString.copyFromUtf8("application/json"))
																								 .setRequest(ByteString.copyFromUtf8(json));
		Promise<MapReduceResult> promise = new Promise<MapReduceResult>();
		try {
			getConnection().write(new RpbRequest(new RpbMessage(MSG_MapRedReq, b.build()), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<byte[]> generateAndSetClientId() throws IOException {
		SecureRandom sr;
		try {
			sr = SecureRandom.getInstance("SHA1PRNG");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
		byte[] data = new byte[6];
		sr.nextBytes(data);
		String clientId = CharsetUtils.asString(Base64.encodeBase64Chunked(data), CharsetUtils.ISO_8859_1);
		RPB.RpbSetClientIdReq.Builder b = RPB.RpbSetClientIdReq.newBuilder()
																													 .setClientId(copyFromUtf8(clientId));
		Promise<byte[]> promise = new Promise<byte[]>();
		try {
			getConnection().write(new RpbRequest(new RpbMessage(MSG_SetClientIdReq, b.build()), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@Override public void setClientId(byte[] clientId) throws IOException {
		throw new UnsupportedOperationException("Setting custom client IDs not yet supported");
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<byte[]> getClientId() throws IOException {
		Promise<byte[]> promise = new Promise<byte[]>();
		try {
			getConnection().write(new RpbRequest(new RpbMessage(MSG_GetClientIdReq, null), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	@SuppressWarnings({"unchecked"})
	@Override public Promise<ServerInfo> getServerInfo() throws IOException {
		Promise<ServerInfo> promise = new Promise<ServerInfo>();
		try {
			getConnection().write(new RpbRequest(new RpbMessage(MSG_GetServerInfoReq, null), promise));
		} catch (Exception e) {
			errorHandler.handleError(e);
		}
		return promise;
	}

	private List<RiakObject> createRiakObjectsFromContent(List<RPB.RpbContent> contents) {
		List<RiakObject> robjs = new ArrayList<RiakObject>(contents.size());
		for (RPB.RpbContent content : contents) {
			RiakObject robj = new RiakObject();
			robj.setVtag(content.getVtag());
			robj.setContentType(content.getContentType());
			robj.setContentEncoding(content.getContentEncoding());
			robj.setLastModified(content.getLastMod());
			robj.setLastModifiedUsec(content.getLastModUsecs());
			robj.setValue(content.getValue());
			robjs.add(robj);
		}
		return robjs;
	}

	private class PendingRequestFilter extends BaseFilter {
		@Override public NextAction handleConnect(FilterChainContext ctx) throws IOException {
			pendingRequests.clear();
			return ctx.getInvokeAction();
		}

		@SuppressWarnings({"unchecked"})
		@Override public NextAction handleRead(final FilterChainContext ctx) throws IOException {
			final RpbMessage<?> msg = ctx.getMessage();
			final RpbRequest pending = pendingRequests.poll();
			if (null == pending) {
				return ctx.getStopAction();
			}
			if (log.isDebugEnabled()) {
				log.debug(String.format("Incoming message: " + msg));
				log.debug(String.format("Pending request: " + pending));
//				log.debug(String.format("requests=" + requests.get() + ", responses=" + responses.incrementAndGet()));
			}

			Object response = null;
			switch (msg.getCode()) {
				case MSG_ErrorResp:
					RPB.RpbErrorResp errorResp = (RPB.RpbErrorResp) msg.getMessage();
					pending.getPromise().setFailure(new RiakException(errorResp.getErrmsg().toStringUtf8()));
					break;
				case MSG_DelResp:
					response = Void.INSTANCE;
					break;
				case MSG_GetBucketResp:
					RPB.RpbGetBucketResp bucketResp = (RPB.RpbGetBucketResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("GetBucket response: %s", bucketResp));
					if (bucketResp.hasProps()) {
						RPB.RpbBucketProps bucketProps = bucketResp.getProps();
						BucketProperties props = new BucketPropertiesBuilder()
								.allowSiblings(bucketProps.getAllowMult())
								.nVal(bucketProps.getNVal())
								.build();
						response = props;
					}
					break;
				case MSG_GetClientIdResp:
					RPB.RpbGetClientIdResp clientIdResp = (RPB.RpbGetClientIdResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("GetClientId response: %s", clientIdResp));
					response = clientIdResp.getClientId().toByteArray();
					break;
				case MSG_GetResp:
					RPB.RpbGetResp getResp = ((RPB.RpbGetResp) msg.getMessage());
					if (log.isDebugEnabled())
						log.debug(String.format("Get response: %s", getResp));
					List<RiakObject> robjs = createRiakObjectsFromContent(getResp.getContentList());
					response = new RiakResponse(getResp.getVclock().toByteArray(), robjs.toArray(new RiakObject[robjs.size()]));
					break;
				case MSG_GetServerInfoResp:
					RPB.RpbGetServerInfoResp serverInfoResp = (RPB.RpbGetServerInfoResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("GetServerInfo response: %s", serverInfoResp));
					response = new ServerInfo(serverInfoResp.getNode().toStringUtf8(),
																		serverInfoResp.getServerVersion().toStringUtf8());
					break;
				case MSG_ListBucketsResp:
					RPB.RpbListBucketsResp listBucketsResp = (RPB.RpbListBucketsResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("ListBuckets response: %s", listBucketsResp));
					Set<String> buckets = new HashSet<String>();
					for (ByteString s : listBucketsResp.getBucketsList()) {
						buckets.add(s.toStringUtf8());
					}
					response = buckets;
					break;
				case MSG_ListKeysResp:
					RPB.RpbListKeysResp listKeysResp = (RPB.RpbListKeysResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("ListKeys response: %s", listKeysResp));
					Set<String> keys = new HashSet<String>();
					for (ByteString s : listKeysResp.getKeysList()) {
						keys.add(s.toStringUtf8());
					}
					response = keys;
					break;
				case MSG_MapRedResp:
					RPB.RpbMapRedResp mapRedResp = (RPB.RpbMapRedResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("MapRed response: %s", mapRedResp));
					String json = mapRedResp.getResponse().toStringUtf8();
					RiakAsyncMapReduceResult mapRedResult = new RiakAsyncMapReduceResult();
					mapRedResult.setResultRaw(json);
					response = mapRedResult;
					break;
				case MSG_PingResp:
					break;
				case MSG_PutResp:
					RPB.RpbPutResp putResp = (RPB.RpbPutResp) msg.getMessage();
					if (log.isDebugEnabled())
						log.debug(String.format("Put response: %s", putResp));
					robjs = createRiakObjectsFromContent(putResp.getContentsList());
					response = new RiakResponse(putResp.getVclock().toByteArray(), robjs.toArray(new RiakObject[robjs.size()]));
					break;
				case MSG_SetBucketResp:
					break;
				case MSG_SetClientIdResp:
					RPB.RpbSetClientIdReq clientIdReq = (RPB.RpbSetClientIdReq) pending.getMessage().getMessage();
					response = clientIdReq.getClientId().toByteArray();
					break;
			}
			// Maybe call callback
			if (null != response)
				pending.getPromise().setResult(response);

			if (log.isDebugEnabled()) {
				long elapsed = System.currentTimeMillis() - pending.getStart();
				String msgClazz = "null";
				if (null != pending.getMessage().getMessage()) {
					msgClazz = pending.getMessage().getMessage().getClass().getSimpleName();
				}
				log.debug(String.format("Round-trip time for %s: %s", msgClazz, elapsed));
			}

			return ctx.getStopAction();
		}

		@Override public NextAction handleWrite(FilterChainContext ctx) throws IOException {
			RpbRequest pending = ctx.getMessage();
			pendingRequests.add(pending);
			ctx.setMessage(pending.getMessage());
			return ctx.getInvokeAction();
		}

		@Override public void exceptionOccurred(FilterChainContext ctx, Throwable error) {
			errorHandler.handleError(error);
		}
	}

	private class RpbRequest<T> {
		private Long start = System.currentTimeMillis();
		private RpbMessage message;
		private Promise<T> promise;

		private RpbRequest(RpbMessage message, Promise<T> promise) {
			this.message = message;
			this.promise = promise;
		}

		public void setStart() {
			start = System.currentTimeMillis();
		}

		public Long getStart() {
			return start;
		}

		public RpbMessage getMessage() {
			return message;
		}

		public Promise<T> getPromise() {
			return promise;
		}

		@Override public String toString() {
			String s = "RpbRequest{" +
					"start=" + start +
					", message=" + message +
					", promise=" + promise + "}";
			return s;
		}
	}

}
