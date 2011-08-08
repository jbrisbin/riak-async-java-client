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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.query.MapReduceResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RiakAsyncMapReduceResult implements MapReduceResult {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private String json;
	private List<?> result;
	private ObjectMapper mapper = new ObjectMapper();

	public RiakAsyncMapReduceResult() {
	}

	public RiakAsyncMapReduceResult(List<?> result) {
		this.result = result;
	}

	public void setResult(List<?> result) {
		this.result = result;
	}

	public void setResultRaw(String json) {
		this.json = json;
		try {
			this.result = mapper.readValue(json, List.class);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}

	@SuppressWarnings({"unchecked"})
	@Override public <T> Collection<T> getResult(Class<T> targetClass) throws ConversionException {
		if (null != result && result.size() > 0 && result.get(0).getClass().isAssignableFrom(targetClass)) {
			return (Collection<T>) result;
		} else if (null != result && result.size() > 0) {
			// Needs conversion
		}
		return Collections.emptyList();
	}

	@Override public String getResultRaw() {
		return json;
	}

}
