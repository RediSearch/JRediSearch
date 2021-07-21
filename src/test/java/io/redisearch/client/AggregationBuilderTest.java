package io.redisearch.client;

import io.redisearch.AggregationResult;
import io.redisearch.Document;
import io.redisearch.FieldName;
import io.redisearch.Schema;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.Row;
import io.redisearch.aggregation.SortedField;
import io.redisearch.aggregation.reducers.Reducers;
import redis.clients.jedis.exceptions.JedisDataException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import redis.clients.jedis.util.SafeEncoder;

public class AggregationBuilderTest extends TestBase {

  @BeforeClass
  public static void prepare() {
      TEST_INDEX = "aggbindex";
      TestBase.prepare();
  }

  @AfterClass
  public static void tearDown() {
      TestBase.tearDown();
  }

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

    Client cl = getDefaultClient();
    Schema sc = new Schema();
    sc.addSortableTextField("name", 1.0);
    sc.addSortableNumericField("count");
    cl.createIndex(sc, Client.IndexOptions.defaultOptions());
    cl.addDocument(new Document("data1").set("name", "abc").set("count", 10));
    cl.addDocument(new Document("data2").set("name", "def").set("count", 5));
    cl.addDocument(new Document("data3").set("name", "def").set("count", 25));

    AggregationBuilder r = new AggregationBuilder()
        .groupBy("@name", Reducers.sum("@count").as("sum"))
        .sortBy(10, SortedField.desc("@sum"));


    // actual search
    AggregationResult res = cl.aggregate(r);
    assertEquals(2, res.totalResults);
    
    Row r1 = res.getRow(0);
    assertNotNull(r1);
    assertEquals("def", r1.getString("name"));
    assertEquals(30, r1.getLong("sum"));
    assertEquals(30., r1.getDouble("sum"), 0);


    assertEquals(0L, r1.getLong("nosuchcol"));
    assertEquals(0.0, r1.getDouble("nosuchcol"), 0);
    assertEquals("", r1.getString("nosuchcol"));


    Row r2 = res.getRow(1);
    assertNotNull(r2);
    assertEquals("abc", r2.getString("name"));
    assertEquals(10, r2.getLong("sum"));
  }

  @Test
  public void testApplyAndFilterAggregations() {
    /**
         127.0.0.1:6379> FT.CREATE test_index SCHEMA name TEXT SORTABLE subj1 NUMERIC SORTABLE subj2 NUMERIC SORTABLE
         OK
         127.0.0.1:6379> FT.ADD test_index data1 1.0 FIELDS name abc subj1 20 subj2 70
         OK
         127.0.0.1:6379> FT.ADD test_index data2 1.0 FIELDS name def subj1 60 subj2 40
         OK
         127.0.0.1:6379> FT.ADD test_index data3 1.0 FIELDS name ghi subj1 50 subj2 80
         OK
         127.0.0.1:6379> FT.ADD test_index data1 1.0 FIELDS name abc subj1 30 subj2 20
         OK
         127.0.0.1:6379> FT.ADD test_index data2 1.0 FIELDS name def subj1 65 subj2 45
         OK
         127.0.0.1:6379> FT.ADD test_index data3 1.0 FIELDS name ghi subj1 70 subj2 70
         OK
     */

    Client cl = getDefaultClient();
    Schema sc = new Schema();
    sc.addSortableTextField("name", 1.0);
    sc.addSortableNumericField("subj1");
    sc.addSortableNumericField("subj2");
    cl.createIndex(sc, Client.IndexOptions.defaultOptions());
    cl.addDocument(new Document("data1").set("name", "abc").set("subj1", 20).set("subj2", 70));
    cl.addDocument(new Document("data2").set("name", "def").set("subj1", 60).set("subj2", 40));
    cl.addDocument(new Document("data3").set("name", "ghi").set("subj1", 50).set("subj2", 80));
    cl.addDocument(new Document("data4").set("name", "abc").set("subj1", 30).set("subj2", 20));
    cl.addDocument(new Document("data5").set("name", "def").set("subj1", 65).set("subj2", 45));
    cl.addDocument(new Document("data6").set("name", "ghi").set("subj1", 70).set("subj2", 70));

    AggregationBuilder r = new AggregationBuilder().apply("(@subj1+@subj2)/2", "attemptavg")
        .groupBy("@name",Reducers.avg("@attemptavg").as("avgscore"))
        .filter("@avgscore>=50")
        .sortBy(10, SortedField.asc("@name"));


    // actual search
    AggregationResult res = cl.aggregate(r);
    assertEquals(3, res.totalResults);
    
    Row r1 = res.getRow(0);
    assertNotNull(r1);
    assertEquals("def", r1.getString("name"));
    assertEquals(52.5, r1.getDouble("avgscore"), 0);


    Row r2 = res.getRow(1);
    assertNotNull(r2);
    assertEquals("ghi", r2.getString("name"));
    assertEquals(67.5, r2.getDouble("avgscore"), 0);
  }

  @Test
  public void testLoadAsAggregations() {
    Client cl = getDefaultClient();
    Schema sc = new Schema();
    sc.addSortableTextField("name", 1.0);
    sc.addSortableNumericField("subj1");
    sc.addSortableNumericField("subj2");
    cl.createIndex(sc, Client.IndexOptions.defaultOptions());
    cl.addDocument(new Document("data1").set("name", "abc").set("subj1", 20).set("subj2", 70));
    cl.addDocument(new Document("data2").set("name", "def").set("subj1", 60).set("subj2", 40));

    AggregationBuilder builder = new AggregationBuilder()
        .load(FieldName.of("@subj1").as("a"), FieldName.of("@subj2").as("b"))
        .apply("(@a+@b)/2", "avg").sortByDesc("@avg");

    AggregationResult result = cl.aggregate(builder);
    assertEquals(50.0, result.getRow(0).getDouble("avg"), 0d);
    assertEquals(45.0, result.getRow(1).getDouble("avg"), 0d);
  }

  @Test
  public void testCursor() throws InterruptedException {
    /**
         127.0.0.1:6379> FT.CREATE test_index SCHEMA name TEXT SORTABLE count NUMERIC SORTABLE
         OK
         127.0.0.1:6379> FT.ADD test_index data1 1.0 FIELDS name abc count 10
         OK
         127.0.0.1:6379> FT.ADD test_index data2 1.0 FIELDS name def count 5
         OK
         127.0.0.1:6379> FT.ADD test_index data3 1.0 FIELDS name def count 25
     */

    Client cl = getDefaultClient();
    Schema sc = new Schema();
    sc.addSortableTextField("name", 1.0);
    sc.addSortableNumericField("count");
    cl.createIndex(sc, Client.IndexOptions.defaultOptions());
    cl.addDocument(new Document("data1").set("name", "abc").set("count", 10));
    cl.addDocument(new Document("data2").set("name", "def").set("count", 5));
    cl.addDocument(new Document("data3").set("name", "def").set("count", 25));

    AggregationBuilder r = new AggregationBuilder()
        .groupBy("@name", Reducers.sum("@count").as("sum"))
        .sortBy(10, SortedField.desc("@sum"))
        .cursor(1, 3000);

    // actual search
    AggregationResult res = cl.aggregate(r);
    assertEquals(2, res.totalResults);
    
    Row row = res.getRow(0);
    assertNotNull(row);
    assertEquals("def", row.getString("name"));
    assertEquals(30, row.getLong("sum"));
    assertEquals(30., row.getDouble("sum"), 0);

    assertEquals(0L, row.getLong("nosuchcol"));
    assertEquals(0.0, row.getDouble("nosuchcol"), 0);
    assertEquals("", row.getString("nosuchcol"));

    res = cl.cursorRead(res.getCursorId(), 1);        
    Row row2 = res.getRow(0);
    assertNotNull(row2);
    assertEquals("abc", row2.getString("name"));
    assertEquals(10, row2.getLong("sum"));

    assertTrue(cl.cursorDelete(res.getCursorId()));

    try {
      cl.cursorRead(res.getCursorId(), 1);
      fail();
    } catch(JedisDataException e) {}

    AggregationBuilder r2 = new AggregationBuilder()
        .groupBy("@name", Reducers.sum("@count").as("sum"))
        .sortBy(10, SortedField.desc("@sum"))
        .cursor(1, 1000);

    Thread.sleep(1000);

    try {
      cl.cursorRead(res.getCursorId(), 1);
      fail();
    } catch(JedisDataException e) {}
  }
  
  @Test
  public void testWrongAggregation() throws InterruptedException {

    Client cl = getDefaultClient();
    Schema sc = new Schema()
        .addTextField("title", 5.0)
        .addTextField("body", 1.0)
        .addTextField("state", 1.0)
        .addNumericField("price");

    cl.createIndex(sc, Client.IndexOptions.defaultOptions());

    // insert document(s)
    Map<String, Object> fields = new HashMap<>();
    fields.put("title", "hello world");
    fields.put("state", "NY");
    fields.put("body", "lorem ipsum");
    fields.put("price", "1337");
    cl.addDocument("doc1", fields);

    // wrong aggregation query
    AggregationBuilder builder = new AggregationBuilder("hello")
        .apply("@price/1000", "k")
        .groupBy("@state", Reducers.avg("@k").as("avgprice"))
        .filter("@avgprice>=2")
        .sortBy(10, SortedField.asc("@state"));

    try {
      cl.aggregate(builder);
      fail();
    }catch(JedisDataException e) {
      // should throw JedisDataException on wrong aggregation query 
    }

  }
}
