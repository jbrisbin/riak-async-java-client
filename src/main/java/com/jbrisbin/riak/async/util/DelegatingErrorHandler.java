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

package com.jbrisbin.riak.async.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.jbrisbin.riak.async.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class DelegatingErrorHandler implements ErrorHandler {

	private Logger log = LoggerFactory.getLogger(getClass());
	private ConcurrentHashMap<Class<? extends Throwable>, ErrorHandler> handlers = new ConcurrentHashMap<Class<? extends Throwable>, ErrorHandler>();

	public DelegatingErrorHandler() {
		handlers.put(Throwable.class, new ErrorHandler() {
			@Override public void handleError(Throwable t) {
				log.error(t.getMessage(), t);
			}
		});
	}

	public void registerErrorHandler(Class<? extends Throwable> clazz, ErrorHandler handler) {
		handlers.put(clazz, handler);
	}

	public ConcurrentHashMap<Class<? extends Throwable>, ErrorHandler> getHandlers() {
		return handlers;
	}

	@Override public void handleError(Throwable t) {
		for (Map.Entry<Class<? extends Throwable>, ErrorHandler> entry : handlers.entrySet()) {
			if (t.getClass().isAssignableFrom(entry.getKey())) {
				entry.getValue().handleError(t);
			}
		}
	}

}
