package io.redisearch.client

import io.redisearch.Query
import io.redisearch.api.SearchClient

class SearchClientSpec extends ClientSpec {


    def setup() {

    }

    def "explain test explain is called"() {
        String indexName = "testIndex"
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


}
