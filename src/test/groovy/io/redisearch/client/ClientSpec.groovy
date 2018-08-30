package io.redisearch.client

import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.rdbc.BinaryClient
import redis.clients.rdbc.Connection
import redis.clients.rdbc.Pool
import spock.lang.Specification

class ClientSpec extends Specification {

    Pool<Connection> pool = Mock(Pool)
    Connection jedis = Mock(Connection)
    BinaryClient binaryClient = Mock(BinaryClient)


    def setup() {
        pool.getResource() >> jedis
        jedis.getClient() >> binaryClient
    }

    def "dropIndex() not interfacing but public make sure false is not ok"() {
        when:
        Client client = new Client("testIndex", pool)
        boolean result = client.dropIndex()

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")

        result
    }

    def "jedis pool creation static"() {
        int max = 30
        when:
        Client client = new Client("myIndex", pool)
        JedisPoolConfig jedisPoolConfig = client.initPoolConfig(max)

        then:
        jedisPoolConfig.getMaxTotal() == max

    }

    def "Constructors work error free minus except API dependent"() {

        when:
        new Client("indexName", "host", 3456, 500, 34)
        new Client("indexName", "host", 3456, 500, 34, "mypass")
        new Client("indexName", "host", 3456)


        then:
        noExceptionThrown()
    }

    def "Constructors API dependent error will be thrown"() {
        Set set = ["sentineladdress:8979"]
        when:
        new Client("indexName", "masterName", set)

        then:
        thrown(JedisConnectionException)

        when:
        new Client("indexName", "masterName", set, 20, 300)

        then:
        thrown(JedisConnectionException)

    }


}
