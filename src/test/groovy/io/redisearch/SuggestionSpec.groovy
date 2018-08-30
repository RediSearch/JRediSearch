package io.redisearch


import spock.lang.Specification

class SuggestionSpec extends Specification {


    def setup() {
    }

    def "Equals and Hashcode contract test"() {
        when:
        Suggestion suggestion1 = Suggestion.builder().score(0.9).str("string").payload("payload").build()
        Suggestion suggestion2 = suggestion1.toBuilder().build()
        Suggestion opposite = Suggestion.builder().score(0.3).str("nope").payload("nope").build()

        then:
        suggestion1.hashCode() == suggestion2.hashCode()
        suggestion1.equals(suggestion1)
        suggestion1.equals(suggestion2)
        !suggestion1.equals(opposite)
        !suggestion1.equals(null)
        !suggestion1.equals(new String("NOPE"))
    }

    def "toString method"() {
        expect:
        Suggestion.builder().score(0.9).str("string").payload("payload")
                .build().toString().length() > 5
    }

    def "Validation"() {
        when:
        Suggestion.builder().score(-0.1).build()

        then:
        thrown(IllegalStateException)

        when:
        Suggestion.builder().score(9).build()

        then:
        thrown(IllegalStateException)
    }

}


