package io.redisearch


import spock.lang.Specification

class DocumentSpec extends Specification {

    def setup() {
    }

    def "Constructor (String id, Map<String,Object> fields)"() {
        Map<String,Object> fields = ["field":"here it is"]

        when:
        Document document = new Document("idvalue", fields )
        document.setScore(1)

        then:
        document.get(fields.get(0)) == fields.get(1)
        document.getScore() == 1
        document.toString() != null
        document.hasProperty("field")
    }


}


