package io.redisearch.client

import io.redisearch.Suggestion
import io.redisearch.api.SuggestionClient
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


    def "addSuggestion with a valid Suggestion with no increment"() {

        when:
        SuggestionClient client = new Client("testIndex", pool)
        Suggestion suggestion = Suggestion.builder().str("suggestion string").score(1.0).build()
        Long i = client.addSuggestion(suggestion, false)

        then:
        1 * binaryClient.getIntegerReply() >> 1
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGADD, _)
        i == 1

    }

    def "getSuggestion with a valid set of options"() {
        SuggestionOptions suggestionOptions = SuggestionOptions.builder().build()
        List<String> values = ["word", "wording", "wording simulation only"]
        when:
        SuggestionClient client = new Client("testIndex", pool)
        List<Suggestion> result = client.getSuggestion("wor", suggestionOptions)

        then:
        1 * binaryClient.getMultiBulkReply() >> values
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGGET, _)
        result.get(0).getString() == values.get(0)

    }

    def "getSuggestion with Payload to be returned"() {
        SuggestionOptions suggestionOptions = SuggestionOptions.builder().with(SuggestionOptions.With.PAYLOAD).build()
        List<String> values = ["word", "Word up I am a payoald", "wording", "wording it up payload"]
        when:
        SuggestionClient client = new Client("testIndex", pool)
        List<Suggestion> result = client.getSuggestion("wo", suggestionOptions)

        then:
        1 * binaryClient.getMultiBulkReply() >> values
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGGET, _)
        result.get(0).getString() == values.get(0)
        result.get(1).getPayload() == values.get(3)

    }


}
