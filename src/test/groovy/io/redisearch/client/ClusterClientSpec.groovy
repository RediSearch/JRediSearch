package io.redisearch.client


import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.rdbc.BinaryClient
import redis.clients.rdbc.Connection
import redis.clients.rdbc.Pool
import spock.lang.Specification

class ClusterClientSpec extends Specification {

    Pool<Connection> pool = Mock(Pool)
    Connection jedis = Mock(Connection)
    BinaryClient binaryClient = Mock(BinaryClient)


    def setup() {
        pool.getResource() >> jedis
        jedis.getClient() >> binaryClient
    }

    def "Ensure broadcast "() {
        def args = ["127.0.0.1", "123.12.1.2"]
        when:
        ClusterClient client = new ClusterClient("testIndex", pool)
        List<Object> result = client.broadcast("127.0.0.1", "123.12.1.2")

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> args

        1 * binaryClient.sendCommand(Commands.ClusterCommand.BROADCAST, args)

        result.size() == 2
    }


    def "Constructors work error free minus except API dependent"() {

        when:
        new ClusterClient("indexName", "host", 3456, 500, 34)
        new ClusterClient("indexName", "host", 3456, 500, 34, "mypass")
        new ClusterClient("indexName", "host", 3456)


        then:
        noExceptionThrown()
    }

    def "Constructors API dependent error will be thrown"() {
        Set set = ["sentineladdress:8979"]
        when:
        new ClusterClient("indexName", "masterName", set)

        then:
        thrown(JedisConnectionException)

        when:
        new ClusterClient("indexName", "masterName", set, 20, 300)

        then:
        thrown(JedisConnectionException)

    }


}
