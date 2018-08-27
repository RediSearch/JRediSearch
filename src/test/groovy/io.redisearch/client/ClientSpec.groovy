package io.redisearch.client

import redis.clients.jedis.JedisPoolConfig
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


}
