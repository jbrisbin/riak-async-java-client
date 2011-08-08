package com.jbrisbin.riak.async

import com.basho.riak.client.raw.RiakResponse
import com.basho.riak.client.raw.StoreMeta
import com.jbrisbin.riak.pbc.RiakObject
import groovy.json.JsonBuilder
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import spock.lang.Specification

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
class RiakAsyncClientSpec extends Specification {

	def TIMEOUT = 5
	RiakAsyncClient client

	def setup() {
		client = new RiakAsyncClient()
	}

	def cleanup() {
		client.close()
	}

	def resolve(f) {
		f.get(TIMEOUT, TimeUnit.SECONDS)
	}

	def "Test get server info"() {

		when:
		def r = resolve client.getServerInfo()

		then:
		null != r

	}

	def "Test storing object"() {

		given:
		def json = new JsonBuilder(["count": 1, "data": ["column": "one"]]).toPrettyString()
		def robj = new RiakObject("bucket", "key", "application/json", json.bytes)

		when:
		def r = resolve client.store(robj, new StoreMeta(null, null, false))

		then:
		null != r

	}

	def "Test fetching object"() {

		when:
		RiakResponse r = resolve client.fetch("bucket", "key")

		then:
		null != r
		r.riakObjects.length > 0
		null != r.riakObjects[0].valueAsString

	}

	def "Test fetching bucket"() {

		when:
		def r = resolve client.fetchBucket("bucket")

		then:
		null != r

	}

	def "Test listing buckets"() {

		when:
		def r = resolve client.listBuckets()

		then:
		null != r
		r.size() > 0

	}

	def "Test listing keys"() {

		when:
		def r = resolve client.listKeys("bucket")

		then:
		null != r

	}

	def "Test deleting object"() {

		when:
		resolve client.delete("bucket", "key")
		def r = resolve client.fetch("bucket", "key")

		then:
		null != r
		r.riakObjects.length == 0

	}

	def "Test store throughput"() {

		given:
		def max = 1000
		def range = 1..max
		long start = System.currentTimeMillis()
		def f

		when:
		range.each {
			i ->
			def robj = new RiakObject("store.throughput", "key$i", "text/plain", "content for test entry $i")
			f = client.store(robj, new StoreMeta(null, null, false))
		}
		range.each {
			i ->
			f = client.delete("store.throughput", "key$i")
		}
		f.get()

		long elapsed = System.currentTimeMillis() - start
		long throughput = (max * 2) / (elapsed / 1000)
		println "ops/sec: $throughput"

		then:
		throughput > 0

	}

}

class JsonTest {
	String test
}
