package io.redisearch.client

import io.redisearch.Document
import io.redisearch.api.DocumentClient

class DocumentClientSpec extends ClientSpec {


    def setup() {

    }

    def "addDocument document only"() {
        Document document = new Document("myid")

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addDocument(document)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result
    }

    def "addDocument(String docId, Map<String, Object> fields) "() {
        Map<String, Object> fields = new HashMap<>()
        fields.put("key", "value")
        //  Document document = new Document("myid", fields, 0.9, "document payload".getBytes())

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addDocument("docId", fields)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "addDocument(Document doc, AddOptions options) partial replace no save"() {
        AddOptions addOptions = new AddOptions()
        addOptions.setNosave()
        addOptions.setLanguage("en-US")
        addOptions.setReplacementPolicy(AddOptions.ReplacementPolicy.PARTIAL)

        Document document = new Document("myid", ["key": "value"], 0.9, "document payload".getBytes())

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addDocument(document, addOptions)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "addDocument(Document doc, AddOptions options) full replace"() {
        AddOptions addOptions = new AddOptions()
        addOptions.setReplacementPolicy(AddOptions.ReplacementPolicy.FULL)
        addOptions.setLanguage("")
        Document document = new Document("!@#ID", ["key": "value"], 0.6, "a diff payload".getBytes())

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addDocument(document, addOptions)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "replaceDocument(String docId, double score, Map<String, Object> fields) "() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.replaceDocument("myDocId", 1, ["field": "myvalue"])

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "updateDocument(String docId, double score, Map<String, Object> fields) "() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.updateDocument("myDocId", 1, ["field": "myvalue"])

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "addDocument(String docId, double score, Map<String, Object> fields)  "() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addDocument("myDocId", 0.3, ["field": "myvalue"])

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADD, _)
        result

    }

    def "delete document with result 1 success was there"() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.deleteDocument("myDocId")

        then:
        1 * binaryClient.getIntegerReply() >> 1
        1 * binaryClient.sendCommand(Commands.Command.DEL, _)
        result

    }

    def "delete document with result 0"() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.deleteDocument("myDocId")

        then:
        1 * binaryClient.getIntegerReply() >> 0
        1 * binaryClient.sendCommand(Commands.Command.DEL, _)
        !result

    }

    def "getDocument by docId no document returned"() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        Document result = client.getDocument("myDocId")

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> null
        1 * binaryClient.sendCommand(Commands.Command.GET, _)
        result == null

    }

    def "getDocument by docId "() {
        def content = ["title".getBytes(), "here is a value".getBytes(), "anotherValue".getBytes(), "I am a string"]

        when:
        DocumentClient client = new Client("testIndex", pool)
        Document result = client.getDocument("myDocId")

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> content
        1 * binaryClient.sendCommand(Commands.Command.GET, _)
        result.getId() == "myDocId"

    }

    def "addHash replace the current "() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addHash("myDocIdToReplace", 1, true)

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        1 * binaryClient.sendCommand(Commands.Command.ADDHASH, _)
        result

    }

    def "addHash replace not OK "() {

        when:
        DocumentClient client = new Client("testIndex", pool)
        boolean result = client.addHash("myDocIdToReplace", 2, false)

        then:
        1 * binaryClient.getStatusCodeReply() >> "BAD"
        1 * binaryClient.sendCommand(Commands.Command.ADDHASH, _)
        !result

    }

}
