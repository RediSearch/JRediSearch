package io.redisearch.client

import io.redisearch.Suggestion
import io.redisearch.api.SuggestionClient

class SuggestionClientSpec extends ClientSpec {


    def setup() {

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

    def "addSuggestion with a valid Suggestion increment"() {

        when:
        SuggestionClient client = new Client("testIndex", pool)
        Suggestion suggestion = Suggestion.builder().str("suggestion string").score(1.0).build()
        Long i = client.addSuggestion(suggestion, true)

        then:
        1 * binaryClient.getIntegerReply() >> 1
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGADD, _)
        i == 1

    }

    def "addSuggestion with a valid Suggestion and a payload "() {

        when:
        SuggestionClient client = new Client("testIndex", pool)
        Suggestion suggestion = Suggestion.builder().str("suggestion string").payload("i am payload").build()
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

    def "getSuggestion with fuzzy set "() {
        SuggestionOptions suggestionOptions = SuggestionOptions.builder().fuzzy().build()
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

    def "getSuggestion with Payload and Scores to be returned"() {
        SuggestionOptions suggestionOptions = SuggestionOptions.builder().with(SuggestionOptions.With.PAYLOAD_AND_SCORES).build()
        List<String> values = ["word", ".5", "this is the payload", "worry", "0.3", "another payload"]
        when:
        SuggestionClient client = new Client("testIndex", pool)
        List<Suggestion> result = client.getSuggestion("wo", suggestionOptions)

        then:
        1 * binaryClient.getMultiBulkReply() >> values
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGGET, _)
        result.get(0).getString() == values.get(0)
        result.get(1).getPayload() == values.get(5)

    }

    def "getSuggestion with Scores to be returned"() {
        SuggestionOptions suggestionOptions = SuggestionOptions.builder().with(SuggestionOptions.With.SCORES).build()
        List<String> values = ["word", ".5", "worry", "0.3"]
        when:
        SuggestionClient client = new Client("testIndex", pool)
        List<Suggestion> result = client.getSuggestion("wo", suggestionOptions)

        then:
        1 * binaryClient.getMultiBulkReply() >> values
        1 * binaryClient.sendCommand(AutoCompleter.Command.SUGGET, _)
        result.get(0).getString() == values.get(0)
        result.get(1).getScore() == new Double(values.get(3)).doubleValue()

    }



}
