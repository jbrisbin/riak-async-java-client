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

package com.jbrisbin.riak.pbc;

import static com.basho.riak.pbc.RiakMessageCodes.*;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.basho.riak.pbc.RPB;
import com.google.protobuf.Message;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.HeapMemoryManager;
import org.glassfish.grizzly.utils.BufferInputStream;
import org.glassfish.grizzly.utils.BufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RpbFilter extends BaseFilter {

	private Logger log = LoggerFactory.getLogger(getClass());
	private HeapMemoryManager heap;
	private AtomicInteger requests = new AtomicInteger(0);
	private AtomicInteger responses = new AtomicInteger(0);

	public RpbFilter(HeapMemoryManager heap) {
		this.heap = heap;
	}

	@Override public NextAction handleRead(FilterChainContext ctx) throws IOException {
		Buffer buffer = ctx.getMessage();
		int size = buffer.getInt();
		int code = buffer.get();
		if (buffer.remaining() < (size - 1)) {
			buffer.rewind();
			if (log.isDebugEnabled())
				log.debug("Not enough data yet: " + buffer.remaining() + " (need " + size + ")");
			return ctx.getStopAction(buffer);
		}

		BufferInputStream bin = new BufferInputStream(buffer);
		switch (code) {
			case MSG_ErrorResp:
				ctx.setMessage(new RpbMessage<RPB.RpbErrorResp>(MSG_ErrorResp, RPB.RpbErrorResp.parseFrom(bin)));
				break;
			case MSG_DelResp:
				ctx.setMessage(new RpbMessage<Message>(MSG_DelResp, null));
				break;
			case MSG_GetBucketResp:
				ctx.setMessage(new RpbMessage<RPB.RpbGetBucketResp>(MSG_GetBucketResp, RPB.RpbGetBucketResp.parseFrom(bin)));
				break;
			case MSG_GetClientIdResp:
				ctx.setMessage(new RpbMessage<RPB.RpbGetClientIdResp>(MSG_GetClientIdResp, RPB.RpbGetClientIdResp.parseFrom(bin)));
				break;
			case MSG_GetResp:
				ctx.setMessage(new RpbMessage<RPB.RpbGetResp>(MSG_GetResp, RPB.RpbGetResp.parseFrom(bin)));
				break;
			case MSG_GetServerInfoResp:
				ctx.setMessage(new RpbMessage<RPB.RpbGetServerInfoResp>(MSG_GetServerInfoResp, RPB.RpbGetServerInfoResp.parseFrom(bin)));
				break;
			case MSG_ListBucketsResp:
				ctx.setMessage(new RpbMessage<RPB.RpbListBucketsResp>(MSG_ListBucketsResp, RPB.RpbListBucketsResp.parseFrom(bin)));
				break;
			case MSG_ListKeysResp:
				ctx.setMessage(new RpbMessage<RPB.RpbListKeysResp>(MSG_ListKeysResp, RPB.RpbListKeysResp.parseFrom(bin)));
				break;
			case MSG_MapRedResp:
				ctx.setMessage(new RpbMessage<RPB.RpbMapRedResp>(MSG_MapRedResp, RPB.RpbMapRedResp.parseFrom(bin)));
				break;
			case MSG_PingResp:
				ctx.setMessage(new RpbMessage<Message>(MSG_PingResp, null));
				break;
			case MSG_PutResp:
				ctx.setMessage(new RpbMessage<RPB.RpbPutResp>(MSG_PutResp, RPB.RpbPutResp.parseFrom(bin)));
				break;
			case MSG_SetBucketResp:
				ctx.setMessage(new RpbMessage<Message>(MSG_SetBucketResp, null));
				break;
			case MSG_SetClientIdResp:
				ctx.setMessage(new RpbMessage<Message>(MSG_SetClientIdResp, null));
				break;
			default:
				ctx.setMessage(null);
		}
		buffer.tryDispose();
		log.debug("read buffer on thread " + Thread.currentThread().getName());
		log.debug(String.format("req=%s, resp=%s", requests.get(), responses.incrementAndGet()));

		return ctx.getInvokeAction();
	}

	@Override public NextAction handleWrite(FilterChainContext ctx) throws IOException {
		RpbMessage<?> msg = ctx.getMessage();
		BufferOutputStream bout = new BufferOutputStream(heap);
		DataOutputStream dout = new DataOutputStream(bout);

		if (null != msg.getMessage()) {
			dout.writeInt(msg.getMessage().getSerializedSize() + 1);
		} else if (msg.getCode() != MSG_PingReq) {
			dout.writeInt(1);
		} else {
			dout.writeInt(0);
		}
		dout.write(msg.getCode());
		if (null != msg.getMessage()) {
			msg.getMessage().writeTo(dout);
		}
		switch (msg.getCode()) {
			case MSG_DelReq:
				break;
			case MSG_GetBucketReq:
				break;
			case MSG_GetClientIdReq:
				break;
			case MSG_GetReq:
				break;
			case MSG_GetServerInfoReq:
				break;
			case MSG_ListBucketsReq:
				break;
			case MSG_ListKeysReq:
				break;
			case MSG_MapRedReq:
				break;
			case MSG_PingReq:
				break;
			case MSG_PutReq:
				break;
			case MSG_SetBucketReq:
				break;
			case MSG_SetClientIdReq:
				break;
		}
		dout.flush();

		final Buffer buffer = bout.getBuffer();
		buffer.trim();
		buffer.rewind();
		ctx.setMessage(buffer);
		log.debug("wrote buffer on thread " + Thread.currentThread().getName());
		log.debug(String.format("req=%s, resp=%s", requests.incrementAndGet(), responses.get()));

		return ctx.getInvokeAction();
	}

}
