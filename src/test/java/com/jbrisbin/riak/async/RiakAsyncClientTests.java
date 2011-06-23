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
import java.util.concurrent.Future;

import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.StoreMeta;
import com.jbrisbin.riak.pbc.RiakObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RiakAsyncClientTests {

	RiakAsyncClient client;

	@Before
	public void setup() {
		client = new RiakAsyncClient();
	}

	@After
	public void cleanup() {
		client.close();
	}

	@Test
	public void testMultipleRequestsAtOnce() throws IOException, ExecutionException, InterruptedException {
		Future<RiakResponse> future = null;
		for (int i = 0; i < 100; i++) {
			RiakObject robj = new RiakObject("store.throughput", "key" + i, "text/plain", "this is content from test " + i);
			client.store(robj, new StoreMeta(null, null, false)).get();
		}
		if (null != future)
			future.get();
//		else
//			throw new IllegalStateException("Future was null!");
	}

}
