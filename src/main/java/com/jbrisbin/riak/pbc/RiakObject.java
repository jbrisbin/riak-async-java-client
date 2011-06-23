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

import static com.google.protobuf.ByteString.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.pbc.RPB;
import com.google.protobuf.ByteString;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class RiakObject implements IRiakObject, Buildable<RPB.RpbContent> {

	private final Object userMetaDataLock = new Object();

	private String bucket;
	private String key;
	private VClock vclock;
	private ByteString vtag;
	private String contentType;
	private String contentEncoding;
	private String charset;
	private List<RiakLink> links = Collections.synchronizedList(new ArrayList<RiakLink>());
	private Map<String, String> userMetaData = new LinkedHashMap<String, String>();
	private Integer lastModified;
	private Integer lastModifiedUsec;
	private ByteString value;
	private RPB.RpbContent.Builder builder = RPB.RpbContent.newBuilder();

	public RiakObject() {
	}

	public RiakObject(String bucket, String key, String contentType, String value) {
		this.bucket = bucket;
		this.key = key;
		setContentType(contentType);
		setValue(value);
	}

	public RiakObject(String bucket, String key, String contentType, byte[] value) {
		this.bucket = bucket;
		this.key = key;
		setContentType(contentType);
		setValue(value);
	}

	public RiakObject(String bucket, String key, byte[] value) {
		this.bucket = bucket;
		this.key = key;
		setValue(value);
	}

	@Override public String getBucket() {
		return bucket;
	}

	@Override public byte[] getValue() {
		return (null != value ? value.toByteArray() : null);
	}

	@Override public String getValueAsString() {
		return (null != value ? value.toStringUtf8() : null);
	}

	@Override public VClock getVClock() {
		return vclock;
	}

	@Override public String getVClockAsString() {
		return (null != vclock ? vclock.asString() : null);
	}

	@Override public String getKey() {
		return key;
	}

	@Override public String getVtag() {
		return (null != vtag ? vtag.toStringUtf8() : null);
	}

	@Override public Date getLastModified() {
		Date d = null;
		if (lastModified != null && lastModifiedUsec != null) {
			long t = (lastModified * 1000L) + (lastModifiedUsec / 100L);
			d = new Date(t);
		}
		return d;
	}

	@Override public String getContentType() {
		return contentType;
	}

	public String getContentEncoding() {
		return contentEncoding;
	}

	public String getCharset() {
		return charset;
	}

	@Override public List<RiakLink> getLinks() {
		return links;
	}

	@Override public boolean hasLinks() {
		return !links.isEmpty();
	}

	@Override public int numLinks() {
		return links.size();
	}

	@Override public boolean hasLink(RiakLink riakLink) {
		return links.contains(riakLink);
	}

	@Override public Map<String, String> getMeta() {
		return userMetaData;
	}

	@Override public boolean hasUsermeta() {
		return !userMetaData.isEmpty();
	}

	@Override public boolean hasUsermeta(String key) {
		return userMetaData.containsKey(key);
	}

	@Override public String getUsermeta(String key) {
		return userMetaData.get(key);
	}

	@Override public Iterable<Map.Entry<String, String>> userMetaEntries() {
		return userMetaData.entrySet();
	}

	public void setValue(ByteString byteString) {
		this.value = byteString;
		this.builder.setValue(byteString);
	}

	@Override public void setValue(byte[] bytes) {
		this.value = copyFrom(bytes);
		this.builder.setValue(this.value);
	}

	@Override public void setValue(String val) {
		if (null != val) {
			this.value = copyFromUtf8(val);
			this.builder.setValue(this.value);
		}
	}

	@Override public void setContentType(String contentType) {
		this.contentType = contentType;
		this.builder.setContentType(copyFromUtf8(contentType));
	}

	public void setContentType(ByteString contentType) {
		this.contentType = contentType.toStringUtf8();
		this.builder.setContentType(contentType);
	}

	public void setContentEncoding(String contentEncoding) {
		this.contentEncoding = contentEncoding;
		this.builder.setContentEncoding(copyFromUtf8(contentEncoding));
	}

	public void setContentEncoding(ByteString contentEncoding) {
		this.contentEncoding = contentEncoding.toStringUtf8();
		this.builder.setContentEncoding(contentEncoding);
	}

	public void setCharset(String charset) {
		this.charset = charset;
		builder.setCharset(copyFromUtf8(charset));
	}

	public void setVtag(ByteString vtag) {
		this.vtag = vtag;
		builder.setVtag(vtag);
	}

	public void setLastModified(Integer lastModified) {
		this.lastModified = lastModified;
	}

	public void setLastModifiedUsec(Integer lastModifiedUsec) {
		this.lastModifiedUsec = lastModifiedUsec;
	}

	@Override public IRiakObject addLink(RiakLink riakLink) {
		links.add(riakLink);
		return this;
	}

	@Override public IRiakObject removeLink(RiakLink riakLink) {
		links.remove(riakLink);
		return this;
	}

	@Override public IRiakObject addUsermeta(String key, String val) {
		userMetaData.put(key, val);
		return this;
	}

	@Override public IRiakObject removeUsermeta(String key) {
		userMetaData.remove(key);
		return this;
	}

	@Override public Iterator<RiakLink> iterator() {
		return links.iterator();
	}

	@Override public RPB.RpbContent build() {
		if (null != lastModified)
			builder.setLastMod(lastModified);
		if (null != lastModifiedUsec)
			builder.setLastModUsecs(lastModifiedUsec);

		for (Map.Entry<String, String> mentry : userMetaData.entrySet()) {
			String mkey = mentry.getKey();
			String mval = mentry.getValue();
			RPB.RpbPair.Builder pairBuilder = RPB.RpbPair.newBuilder();
			pairBuilder.setKey(copyFromUtf8(mkey));
			pairBuilder.setValue(copyFromUtf8(mval));
			builder.addUsermeta(pairBuilder);
		}

		for (RiakLink riakLink : links) {
			RPB.RpbLink.Builder linkBuilder = RPB.RpbLink.newBuilder();
			linkBuilder.setBucket(copyFromUtf8(riakLink.getBucket()));
			linkBuilder.setKey(copyFromUtf8(riakLink.getKey()));
			linkBuilder.setTag(copyFromUtf8(riakLink.getTag()));
			builder.addLinks(linkBuilder);
		}

		return builder.build();
	}

	@Override public String toString() {
		return "RiakObject{" +
				"userMetaDataLock=" + userMetaDataLock +
				", bucket=" + bucket +
				", key=" + key +
				", vclock=" + vclock +
				", vtag=" + vtag +
				", contentType='" + contentType + '\'' +
				", contentEncoding='" + contentEncoding + '\'' +
				", charset='" + charset + '\'' +
				", links=" + links +
				", userMetaData=" + userMetaData +
				", lastModified=" + lastModified +
				", lastModifiedUsec=" + lastModifiedUsec +
				", value=" + value +
				", builder=" + builder +
				'}';
	}
}
