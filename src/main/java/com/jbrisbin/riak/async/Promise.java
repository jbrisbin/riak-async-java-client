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

import java.util.Queue;
import java.util.ResourceBundle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class Promise<T> implements Future<T> {

	private static final String CANCELLED = "PromiseCancelled";
	private static final ResourceBundle messages = ResourceBundle.getBundle(
			"com.jbrisbin.riak.async.RiakAsyncClientResources");

	private ArrayBlockingQueue<T> resultQueue = new ArrayBlockingQueue<T>(1);
	private AtomicReference<CompletionHandler<T>> completionHandler = new AtomicReference<CompletionHandler<T>>();
	private AtomicReference<Throwable> failure = new AtomicReference<Throwable>();

	public void setCompletionHandler(CompletionHandler<T> completionHandler) {
		this.completionHandler.set(completionHandler);
		if (!resultQueue.isEmpty()) {
			completionHandler.complete(resultQueue.peek());
		}
	}

	public CompletionHandler<T> getCompletionHandler() {
		return completionHandler.get();
	}

	public void setFailure(Throwable t) {
		this.failure.set(t);
		CompletionHandler<T> handler = completionHandler.get();
		if (null != handler) {
			handler.failed(t);
		}
	}

	/**
	 * Set the result of this Promise.
	 *
	 * @param obj result to set
	 * @return true if possible, false if result has already been set
	 */
	public boolean setResult(T obj) {
		if (null != resultQueue && resultQueue.size() == 0) {
			boolean b = resultQueue.offer(obj);
			CompletionHandler<T> handler = completionHandler.get();
			if (null != handler) {
				handler.complete(obj);
			}
			return b;
		} else {
			return false;
		}
	}

	public Queue<T> getResultQueue() {
		return resultQueue;
	}

	@Override public boolean cancel(boolean force) {
		if (null == resultQueue) {
			throw new IllegalStateException(messages.getString(CANCELLED));
		}
		boolean b = resultQueue.isEmpty();
		resultQueue = null;
		CompletionHandler<T> handler = completionHandler.get();
		if (null != handler) {
			handler.canceled();
		}
		return b;
	}

	@Override public boolean isCancelled() {
		return null == resultQueue;
	}

	@Override public boolean isDone() {
		if (null == resultQueue) {
			throw new IllegalStateException(messages.getString(CANCELLED));
		}
		return !resultQueue.isEmpty();
	}

	@Override public T get() throws InterruptedException, ExecutionException {
		if (null == resultQueue) {
			throw new IllegalStateException(messages.getString(CANCELLED));
		}
		return resultQueue.take();
	}

	@Override public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (null == resultQueue) {
			throw new IllegalStateException(messages.getString(CANCELLED));
		}
		return resultQueue.poll(l, timeUnit);
	}

}
