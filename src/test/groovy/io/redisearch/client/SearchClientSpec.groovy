package io.redisearch.client

import io.redisearch.AggregationResult
import io.redisearch.Query
import io.redisearch.Schema
import io.redisearch.SearchResult
import io.redisearch.aggregation.AggregationRequest
import io.redisearch.aggregation.Row
import io.redisearch.api.IndexClient
import io.redisearch.api.SearchClient

class SearchClientSpec extends ClientSpec {

    String indexName = "testIndex"

    def setup() {

    }

    def "explain test explain is called"() {

        String statusCodeReply = "INTERSECT {\n" +
                "  @f3:UNION {\n" +
                "    @f3:f3_val\n" +
                "    @f3:<FFL(expanded)\n" +
                "    @f3:+f3_val(expanded)\n" +
                "  }\n" +
                "  @f2:UNION {\n" +
                "    @f2:f2_val\n" +
                "    @f2:<FFL(expanded)\n" +
                "    @f2:+f2_val(expanded)\n" +
                "  }\n" +
                "  @f1:UNION {\n" +
                "    @f1:f1_val\n" +
                "    @f1:<FFL(expanded)\n" +
                "    @f1:+f1_val(expanded)\n" +
                "  }\n" +
                "}"

        when:
        Query query = new Query("search text")
        SearchClient client = new Client(indexName, pool)
        String result = client.explain(query)

        then:
        1 * binaryClient.getStatusCodeReply() >> statusCodeReply
        1 * binaryClient.sendCommand(Commands.Command.EXPLAIN, _)
        result == statusCodeReply
    }


    def "basic search for word with no content"() {
        String id = "record:id"

        when:
        Query query = new Query("search text")
        query.setNoContent()
        SearchClient client = new Client(indexName, pool)
        SearchResult result = client.search(query)

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> [1L, id.getBytes()]
        1 * binaryClient.sendCommand(Commands.Command.SEARCH, _)
        result.totalResults == 1
        result.docs.get(0).getId() == id
    }

    def "basic search for word with content"() {
        String id = "record:id"
        def value = ["title".getBytes(), "here is a value".getBytes()]

        when:
        Query query = new Query("search text")
        SearchClient client = new Client(indexName, pool)
        SearchResult result = client.search(query)

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> [1L, id.getBytes(), value]
        1 * binaryClient.sendCommand(Commands.Command.SEARCH, _)
        result.totalResults == 1
        result.docs.get(0).getId() == id
    }

    def "basic search for word with payload and score"() {
        String id = "record:id"
        String payload = "I am a payload"
        def content = ["title".getBytes(), "here is a value".getBytes()]
        String score = "4.445670127868652"

        when:
        Query query = new Query("search text")
        query.setWithPaload()
        query.setWithScores()
        SearchClient client = new Client(indexName, pool)
        SearchResult result = client.search(query)

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> [1L, id.getBytes(), score.getBytes(), payload.getBytes(), content]
        1 * binaryClient.sendCommand(Commands.Command.SEARCH, _)
        result.totalResults == 1
        result.docs.get(0).getId() == id
        result.docs.get(0).getPayload() == payload.getBytes()
        result.docs.get(0).getScore() == Double.parseDouble(score)
    }

    def "basic aggregation test"() {

        def result = ["key1".getBytes(), "value1".getBytes(),"key2".getBytes(), "1".getBytes()]

        when:
        AggregationRequest aggregationRequest = new AggregationRequest("a term")
        SearchClient client = new Client(indexName, pool)
        AggregationResult resultAnswer = client.aggregate(aggregationRequest)

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> [1L, result]
        1 * binaryClient.sendCommand(Commands.Command.AGGREGATE, _)
        resultAnswer.getResults().size() == 1
        Row row = resultAnswer.getRow(0)
        row.getString("key1") == "value1"
    }


    def "Test out the flags being set in IndexOptions with the default which needs to be passed in"() {
        List list = new ArrayList()
        when:
        new Client.IndexOptions(0x0).serializeRedisArgs(list)

        then:
        noExceptionThrown()
        list.size() == 3

    }

}
