package io.redisearch.client


import io.redisearch.api.IndexClient
import redis.clients.jedis.exceptions.JedisDataException

class IndexClientSpec extends ClientSpec {


    def setup() {

    }

    def "getInfo with success of mapping different objects takes place"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        Map result = client.getInfo()

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> [new String("key_here").getBytes(), new ArrayList()
                                                       , new String("another_key").getBytes()
                                                       , new Integer(2), new String("string_key").getBytes()
                                                       , new String("I am a string value").getBytes()]
        1 * binaryClient.sendCommand(Commands.Command.INFO, _)
        result.get("string_key") == "I am a string value"
        result.get("another_key") == 2

    }

    def "dropIndex false"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        boolean result = client.dropIndex(false)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")
        result

    }

    def "dropIndex true"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        boolean result = client.dropIndex(true)

        then:
        1 * binaryClient.getStatusCodeReply() >> "FALSE"

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")
        !result

    }

    def "dropIndex false with missing index exception"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        client.dropIndex(false)

        then:
        1 * binaryClient.getStatusCodeReply() >> { throw new JedisDataException("unknown") }

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")

        thrown(JedisDataException)
    }

    def "dropIndex true with missing index exception and unknown"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        boolean result = client.dropIndex(true)

        then:
        1 * binaryClient.getStatusCodeReply() >> { throw new JedisDataException("unknown") }

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")

        !result
    }

    def "dropIndex true with missing index exception but exception does not contain unknown"() {
        when:
        IndexClient client = new Client("testIndex", pool)
        client.dropIndex(true)

        then:
        1 * binaryClient.getStatusCodeReply() >> { throw new JedisDataException("not containing special word") }

        1 * binaryClient.sendCommand(Commands.Command.DROP, "testIndex")

        thrown(JedisDataException)
    }


}
