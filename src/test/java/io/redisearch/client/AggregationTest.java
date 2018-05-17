package io.redisearch.client;

import io.redisearch.AggregationResult;
import io.redisearch.Document;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.aggregation.Row;
import io.redisearch.aggregation.SortedField;
import io.redisearch.aggregation.reducers.Reducers;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.*;

/**
 * Created by mnunberg on 5/17/18.
 */
public class AggregationTest extends  ClientTest {
    @Test
    public void testAggregations() {
        /**
         127.0.0.1:6379> FT.CREATE test_index SCHEMA name TEXT SORTABLE count NUMERIC SORTABLE
         OK
         127.0.0.1:6379> FT.ADD test_index data1 1.0 FIELDS name abc count 10
         OK
         127.0.0.1:6379> FT.ADD test_index data2 1.0 FIELDS name def count 5
         OK
         127.0.0.1:6379> FT.ADD test_index data3 1.0 FIELDS name def count 25
         */

        Client cl = getClient();
        Schema sc = new Schema();
        sc.addSortableTextField("name", 1.0);
        sc.addSortableNumericField("count");
        cl.createIndex(sc, Client.IndexOptions.Default());
        cl.addDocument(new Document("data1").set("name", "abc").set("count", 10));
        cl.addDocument(new Document("data2").set("name", "def").set("count", 5));
        cl.addDocument(new Document("data3").set("name", "def").set("count", 25));

        AggregationRequest r = new AggregationRequest()
                .groupBy("@name", Reducers.sum("@count").as("sum"))
                .sortBy(SortedField.desc("@sum"), 10);


        // actual search
        AggregationResult res = cl.aggregate(r);
        Row r1 = res.getRow(0);
        assertNotNull(r1);
        assertEquals("def", r1.getString("name"));
        assertEquals(30, r1.getLong("sum"));

        Row r2 = res.getRow(1);
        assertNotNull(r2);
        assertEquals("abc", r2.getString("name"));
        assertEquals(10, r2.getLong("sum"));
    }
}
