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

import java.util.ListResourceBundle;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RiakAsyncClientResources extends ListResourceBundle {
	@Override protected Object[][] getContents() {
		return new Object[][]{
				{"PromiseCancelled", "This promise has already been cancelled."},
				{"DebugHandlerCancelled", "CompletionHandler.cancelled()"},
				{"DebugHandlerFailed", "CompletionHandler.failed()"},
				{"DebugHandlerComplete", "CompletionHandler.complete()"},
				{"EntryNotFoundException", "Entry %s:%s not found"}
		};
	}
}
