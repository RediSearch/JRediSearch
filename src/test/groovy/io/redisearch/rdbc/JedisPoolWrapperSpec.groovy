package io.redisearch.rdbc

import redis.clients.jedis.Jedis
import redis.clients.jedis.util.Pool
import spock.lang.Specification

class JedisPoolWrapperSpec extends Specification {

    Pool<Jedis> pool = Mock(Pool)

    def setup() {
    }

    def "Make sure wrapper is returning correct wrapped object"() {
        when:
        JedisPoolWrapper jedisPoolWrapper = new JedisPoolWrapper(pool)

        then:
        jedisPoolWrapper.getResource() instanceof JedisWrapper

    }

    def "Make sure the wrapper calls the underlying wrapped Pool"() {
        when:
        JedisPoolWrapper jedisPoolWrapper = new JedisPoolWrapper(pool)
        jedisPoolWrapper.close()

        then:
        1 * pool.close()
    }


}
