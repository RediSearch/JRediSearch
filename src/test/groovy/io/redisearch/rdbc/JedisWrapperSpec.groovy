package io.redisearch.rdbc

import redis.clients.jedis.Jedis
import spock.lang.Specification

class JedisWrapperSpec extends Specification {

    Jedis jedis = Mock(Jedis)

    def setup() {
    }

    def "Make sure wrapper is returning correct wrapped object"() {
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)

        then:
        jedisWrapper.getClient() instanceof BinaryClientWrapper

    }

    def "Make sure the wrapper calls the underlying wrapped close method"() {
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)
        jedisWrapper.close()

        then:
        1 * jedis.close()
    }

    def "Make sure the wrapper calls the underlying wrapped flushDB method"() {
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)
        jedisWrapper.flushDB()

        then:
        1 * jedis.flushDB()
    }

    def "Make sure the wrapper calls the underlying wrapped ping method"() {
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)
        jedisWrapper.ping("message")

        then:
        1 * jedis.ping("message")
    }

    def "Make sure the wrapper calls the underlying wrapped keys method"() {
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)
        jedisWrapper.keys("*/pat")

        then:
        1 * jedis.keys("*/pat")
    }

    def "Make sure the wrapper calls the underlying wrapped hmset method"() {
        Map<String, String> map = ["key": "value"]
        when:
        JedisWrapper jedisWrapper = new JedisWrapper(jedis)
        jedisWrapper.hmset("key", map)

        then:
        1 * jedis.hmset("key", map)
    }
}
