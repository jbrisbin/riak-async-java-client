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

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.glassfish.grizzly.CompletionHandler;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ConnectionPool {

	private final String connectionPoolMutex = "MUTEX";
	private final Logger log = LoggerFactory.getLogger(getClass());

	private String host;
	private Integer port;
	private Long timeout = 30L;
	private TCPNIOTransport transport;
	private ErrorHandler errorHandler;
	private LinkedBlockingQueue<Connection> pool = new LinkedBlockingQueue<Connection>();
	private AtomicInteger maxPoolSize = new AtomicInteger(0);
	private AtomicInteger leased = new AtomicInteger(0);

	public ConnectionPool(String host,
												Integer port,
												Long timeout,
												TCPNIOTransport transport,
												ErrorHandler errorHandler,
												Integer maxPoolSize) {
		this.host = host;
		this.port = port;
		this.timeout = timeout;
		this.transport = transport;
		this.errorHandler = errorHandler;
		this.maxPoolSize.set(maxPoolSize);
	}

	public void setMaxPoolSize(Integer maxPoolSize) {
		this.maxPoolSize.set(maxPoolSize);
	}

	public Connection getConnection() throws IOException {
		try {
			Connection conn;
			if (pool.isEmpty() && leased.get() < maxPoolSize.get()) {
				conn = createNewConnection();
			} else {
				conn = pool.poll(timeout, TimeUnit.SECONDS);
			}
			if (log.isDebugEnabled()) {
				log.debug("Leasing connection " + conn);
			}
			return conn;
		} catch (InterruptedException e) {
			errorHandler.handleError(e);
		} catch (ExecutionException e) {
			errorHandler.handleError(e);
		} catch (TimeoutException e) {
			errorHandler.handleError(e);
		}
		throw new RiakException("Connection pool exhausted and no connection available within " + timeout + " seconds");
	}

	public void release(Connection conn) {
		try {
			pool.offer(conn, timeout, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			errorHandler.handleError(e);
		}
		leased.decrementAndGet();
	}

	public void close(CompletionHandler<ConnectionPool> callback) throws IOException {
		for (Connection conn : pool) {
			try {
				conn.close().get(timeout, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				callback.failed(e);
			} catch (ExecutionException e) {
				callback.failed(e);
			} catch (TimeoutException e) {
				callback.failed(e);
			}
		}
		pool.clear();
		callback.completed(this);
	}

	private Connection createNewConnection() throws IOException, ExecutionException, TimeoutException, InterruptedException {
		Connection conn = transport.connect(host, port).get(timeout, TimeUnit.SECONDS);
		leased.incrementAndGet();
		return conn;
	}

}
