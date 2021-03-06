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

import com.google.protobuf.Message;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RpbMessage<T extends Message> {

	private int code;
	private T message;

	public RpbMessage(int code, T message) {
		this.code = code;
		this.message = message;
	}

	public int getCode() {
		return code;
	}

	public T getMessage() {
		return message;
	}

	@Override public String toString() {
		return "RpbMessage{" +
				"code=" + code +
				", message=" + message +
				'}';
	}
}
