package io.redisearch.client

import io.redisearch.Suggestion
import redis.clients.jedis.Jedis
import redis.clients.jedis.util.Pool
import spock.lang.Specification

class ClientSpec extends Specification {

    Pool<Jedis> pool = Mock(Pool)
    Jedis jedis = Mock(Jedis)
    redis.clients.jedis.Client binaryClient = Mock(redis.clients.jedis.Client)


    def setup() {
        pool.getResource() >> jedis
        jedis.getClient() >> binaryClient
    }

    def "getInfo with success of mapping different objects takes place"() {
        when:
        Client client = new Client("testIndex", pool)
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

    def "addSuggestion with a valid Suggestion with no increment"() {

        when:
        Client client = new Client("testIndex", pool)
        Suggestion suggestion = Suggestion.builder().str("suggestion string").score(1.0).build()
        Long i = client.addSuggestion(suggestion, false)

        then:
        1 * binaryClient.getIntegerReply() >> 1
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGADD, _)
        i == 1

    }

}
