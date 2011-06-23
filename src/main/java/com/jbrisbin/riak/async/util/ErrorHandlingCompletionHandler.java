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

import com.jbrisbin.riak.async.ErrorHandler;
import org.glassfish.grizzly.CompletionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class ErrorHandlingCompletionHandler<T> implements CompletionHandler<T> {

	protected final Logger log = LoggerFactory.getLogger(getClass());
	protected ErrorHandler errorHandler;

	public ErrorHandlingCompletionHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	@Override public void cancelled() {
		if (log.isDebugEnabled()) {
			log.debug("cancelled()");
		}
	}

	@Override public void failed(Throwable throwable) {
		errorHandler.handleError(throwable);
	}

	@Override public void completed(T result) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("completed(): %s", result));
		}
	}

	@Override public void updated(T result) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("updated(): %s", result));
		}
	}

}
