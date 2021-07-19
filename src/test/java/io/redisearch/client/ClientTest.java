package io.redisearch.client;

import io.redisearch.*;
import io.redisearch.Schema.TagField;
import io.redisearch.Schema.TextField;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.redisearch.client.util.ClientUtil.toStringMap;
import static org.junit.Assert.*;

public class ClientTest extends TestBase {

    @BeforeClass
    public static void prepare() {
        TEST_INDEX = "aggregation-builder";
        TestBase.prepare();
    }

    @AfterClass
    public static void tearDown() {
        TestBase.tearDown();
    }

    private static Map<String, Object> toMap(Object... values) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put((String) values[i], values[i + 1]);
        }
        return map;
    }

    private static Map<String, String> toMap(String... values) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put(values[i], values[i + 1]);
        }
        return map;
    }

    @Test
    public void creatDefinion() throws Exception {
        Client cl = getDefaultClient();
        Schema sc = new Schema().addTextField("first", 1.0).addTextField("last", 1.0).addNumericField("age");
        IndexDefinition rule = new IndexDefinition()
                .setFilter("@age>16")
                .setPrefixes(new String[]{"student:", "pupil:"});

        try {
            assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(rule)));
        } catch (JedisDataException e) {
            // ON was only supported from RediSearch 2.0
            assertEquals("Unknown argument `ON`", e.getMessage());
            return;
        }

        try (Jedis jedis = cl.connection()) {
            jedis.hset("profesor:5555", toMap("first", "Albert", "last", "Blue", "age", "55"));
            jedis.hset("student:1111", toMap("first", "Joe", "last", "Dod", "age", "18"));
            jedis.hset("pupil:2222", toMap("first", "Jen", "last", "Rod", "age", "14"));
            jedis.hset("student:3333", toMap("first", "El", "last", "Mark", "age", "17"));
            jedis.hset("pupil:4444", toMap("first", "Pat", "last", "Shu", "age", "21"));
            jedis.hset("student:5555", toMap("first", "Joen", "last", "Ko", "age", "20"));
            jedis.hset("teacher:6666", toMap("first", "Pat", "last", "Rod", "age", "20"));
        }

        SearchResult noFilters = cl.search(new Query());
        assertEquals(4, noFilters.totalResults);

        SearchResult res1 = cl.search(new Query("@first:Jo*"));
        assertEquals(2, res1.totalResults);

        SearchResult res2 = cl.search(new Query("@first:Pat"));
        assertEquals(1, res2.totalResults);

        SearchResult res3 = cl.search(new Query("@last:Rod"));
        assertEquals(0, res3.totalResults);
    }

    @Test
    public void withObjectMap() throws Exception {
        Schema sc = new Schema().addTextField("first", 1.0).addTextField("last", 1.0).addNumericField("age");
        assertTrue(search.createIndex(sc, Client.IndexOptions.defaultOptions()));

        try (Jedis jedis = search.connection()) {
            jedis.hset("student:1111", toStringMap(toMap("first", "Joe", "last", "Dod", "age", 18)));
            jedis.hset("student:3333", toStringMap(toMap("first", "El", "last", "Mark", "age", 17)));
            jedis.hset("pupil:4444", toStringMap(toMap("first", "Pat", "last", "Shu", "age", 21)));
            jedis.hset("student:5555", toStringMap(toMap("first", "Joen", "last", "Ko", "age", 20)));
        }

        SearchResult noFilters = search.search(new Query());
        assertEquals(4, noFilters.totalResults);

        SearchResult res1 = search.search(new Query("@first:Jo*"));
        assertEquals(2, res1.totalResults);

        SearchResult res2 = search.search(new Query("@first:Pat"));
        assertEquals(1, res2.totalResults);

        SearchResult res3 = search.search(new Query("@last:Rod"));
        assertEquals(0, res3.totalResults);
    }

    @Test
    public void search() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0).addTextField("body", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        fields.put("body", "lorem ipsum");
        for (int i = 0; i < 100; i++) {
            assertTrue(cl.addDocument(String.format("doc%d", i), (double) i / 100.0, fields));
        }

        SearchResult res = cl.search(new Query("hello world").limit(0, 5).setWithScores());
        assertEquals(100, res.totalResults);
        assertEquals(5, res.docs.size());
        for (Document d : res.docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertTrue(d.getScore() < 100);
            assertEquals(
                String.format(
                "{\"id\":\"%s\",\"score\":%s,\"properties\":{\"title\":\"hello world\",\"body\":\"lorem ipsum\"}}", 
                d.getId(), Double.toString(d.getScore())),
                d.toString());
        }

        assertTrue(cl.deleteDocument("doc0", true));
        assertFalse(cl.deleteDocument("doc0"));

        res = cl.search(new Query("hello world"));
        assertEquals(99, res.totalResults);

        assertTrue(cl.dropIndex());
        boolean threw = false;
        try {
            res = cl.search(new Query("hello world"));

        } catch (Exception e) {
            threw = true;
        }
        assertTrue(threw);
    }
    
    @Test
    public void searchBatch() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0).addTextField("body", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        fields.put("body", "lorem ipsum");
        for (int i = 0; i < 50; i++) {
          fields.put("title", "hello world");
            assertTrue(cl.addDocument(String.format("doc%d", i), (double) i / 100.0, fields));
        }
        
        for (int i = 50; i < 100; i++) {
          fields.put("title", "good night");
            assertTrue(cl.addDocument(String.format("doc%d", i), (double) i / 100.0, fields));
        }

        SearchResult[] res = cl.searchBatch(
            new Query("hello world").limit(0, 5).setWithScores(),
            new Query("good night").limit(0, 5).setWithScores()
            );
        
        assertEquals(2, res.length);
        assertEquals(50, res[0].totalResults);
        assertEquals(50, res[1].totalResults);
        assertEquals(5, res[0].docs.size());
        for (Document d : res[0].docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertTrue(d.getScore() < 100);
            assertEquals(
                String.format(
                "{\"id\":\"%s\",\"score\":%s,\"properties\":{\"title\":\"hello world\",\"body\":\"lorem ipsum\"}}", 
                d.getId(), Double.toString(d.getScore())),
                d.toString());
        }    
    }


    @Test
    public void testNumericFilter() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0).addNumericField("price");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");

        for (int i = 0; i < 100; i++) {
            fields.put("price", i);
            assertTrue(cl.addDocument(String.format("doc%d", i), fields));
        }

        SearchResult res = cl.search(new Query("hello world").
                addFilter(new Query.NumericFilter("price", 0, 49)));
        assertEquals(50, res.totalResults);
        assertEquals(10, res.docs.size());
        for (Document d : res.docs) {
            long price = Long.valueOf((String) d.get("price"));
            assertTrue(price >= 0);
            assertTrue(price <= 49);
        }

        res = cl.search(new Query("hello world").
                addFilter(new Query.NumericFilter("price", 0, true, 49, true)));
        assertEquals(48, res.totalResults);
        assertEquals(10, res.docs.size());
        for (Document d : res.docs) {
            long price = Long.valueOf((String) d.get("price"));
            assertTrue(price > 0);
            assertTrue(price < 49);
        }
        res = cl.search(new Query("hello world").
                addFilter(new Query.NumericFilter("price", 50, 100)));
        assertEquals(50, res.totalResults);
        assertEquals(10, res.docs.size());
        for (Document d : res.docs) {
            long price = Long.valueOf((String) d.get("price"));
            assertTrue(price >= 50);
            assertTrue(price <= 100);
        }

        res = cl.search(new Query("hello world").
                addFilter(new Query.NumericFilter("price", 20, Double.POSITIVE_INFINITY)));
        assertEquals(80, res.totalResults);
        assertEquals(10, res.docs.size());

        res = cl.search(new Query("hello world").
                addFilter(new Query.NumericFilter("price", Double.NEGATIVE_INFINITY, 10)));
        assertEquals(11, res.totalResults);
        assertEquals(10, res.docs.size());

    }

    @Test
    public void testStopwords() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0);


        assertTrue(cl.createIndex(sc,
                Client.IndexOptions.defaultOptions().setStopwords("foo", "bar", "baz")));

        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world foo bar");
        assertTrue(cl.addDocument("doc1", fields));
        SearchResult res = cl.search(new Query("hello world"));
        assertEquals(1, res.totalResults);
        res = cl.search(new Query("foo bar"));
        assertEquals(0, res.totalResults);

        cl.connection().flushDB();

        assertTrue(cl.createIndex(sc,
                Client.IndexOptions.defaultOptions().setNoStopwords()));
        fields.put("title", "hello world foo bar to be or not to be");
        assertTrue(cl.addDocument("doc1", fields));

        assertEquals(1, cl.search(new Query("hello world")).totalResults);
        assertEquals(1, cl.search(new Query("foo bar")).totalResults);
        assertEquals(1, cl.search(new Query("to be or not to be")).totalResults);
    }


    @Test
    public void testGeoFilter() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0).addGeoField("loc");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        fields.put("loc", "-0.441,51.458");
        assertTrue(cl.addDocument("doc1", fields));
        fields.put("loc", "-0.1,51.2");
        assertTrue(cl.addDocument("doc2", fields));

        SearchResult res = cl.search(new Query("hello world").
                addFilter(
                        new Query.GeoFilter("loc", -0.44, 51.45,
                                10, Query.GeoFilter.KILOMETERS)
                ));

        assertEquals(1, res.totalResults);
        res = cl.search(new Query("hello world").
                addFilter(
                        new Query.GeoFilter("loc", -0.44, 51.45,
                                100, Query.GeoFilter.KILOMETERS)
                ));
        assertEquals(2, res.totalResults);
    }

    // TODO: This test was broken in master branch
    @Test
    public void testPayloads() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        String payload = "foo bar";
        assertTrue(cl.addDocument("doc1", 1.0, fields, false, false, payload.getBytes()));

        SearchResult res = cl.search(new Query("hello world").setWithPayload());
        assertEquals(1, res.totalResults);
        assertEquals(1, res.docs.size());

        assertEquals(payload, new String(res.docs.get(0).getPayload()));
    }

    @Test
    public void testQueryFlags() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();


        for (int i = 0; i < 100; i++) {
            fields.put("title", i % 2 != 0 ? "hello worlds" : "hello world");
            assertTrue(cl.addDocument(String.format("doc%d", i), (double) i / 100.0, fields));
        }

        Query q = new Query("hello").setWithScores();
        SearchResult res = cl.search(q);

        assertEquals(100, res.totalResults);
        assertEquals(10, res.docs.size());

        for (Document d : res.docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertNotEquals(1.0, d.getScore());
            assertTrue(((String) d.get("title")).startsWith("hello world"));
        }

        q = new Query("hello").setNoContent();
        res = cl.search(q);
        for (Document d : res.docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertEquals(1.0, d.getScore(), 0);
            assertEquals(null, d.get("title"));
        }

        // test verbatim vs. stemming
        res = cl.search(new Query("hello worlds"));
        assertEquals(100, res.totalResults);
        res = cl.search(new Query("hello worlds").setVerbatim());
        assertEquals(50, res.totalResults);

        res = cl.search(new Query("hello a world").setVerbatim());
        assertEquals(50, res.totalResults);
        res = cl.search(new Query("hello a worlds").setVerbatim());
        assertEquals(50, res.totalResults);
        res = cl.search(new Query("hello a world").setVerbatim().setNoStopwords());
        assertEquals(0, res.totalResults);
    }

    @Test
    public void testSortQueryFlags() throws Exception {
        Client cl = getDefaultClient();
        Schema sc = new Schema().addSortableTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();

        fields.put("title", "b title");
        cl.addDocument("doc1", 1.0, fields, false, true, null);

        fields.put("title", "a title");
        cl.addDocument("doc2", 1.0, fields, false, true, null);

        fields.put("title", "c title");
        cl.addDocument("doc3", 1.0, fields, false, true, null);

        Query q = new Query("title").setSortBy("title", true);
        SearchResult res = cl.search(q);

        assertEquals(3, res.totalResults);
        Document doc1 = res.docs.get(0);
        assertEquals("a title", doc1.get("title"));

        doc1 = res.docs.get(1);
        assertEquals("b title", doc1.get("title"));

        doc1 = res.docs.get(2);
        assertEquals("c title", doc1.get("title"));
    }

    @Test
    public void testAddHash() throws Exception {
        Client cl = getDefaultClient();
        Jedis conn = cl.connection();
        Schema sc = new Schema().addTextField("title", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        HashMap<String, String> hm = new HashMap<>();
        hm.put("title", "hello world");
        conn.hmset("foo", hm);

        try {
          assertTrue(cl.addHash("foo", 1, false));
        }catch(JedisDataException e) {
          assertTrue(e.getMessage().startsWith("ERR unknown command `FT.ADDHASH`"));
          return; // Starting from RediSearch 2.0 this command is not supported anymore
        }
        SearchResult res = cl.search(new Query("hello world").setVerbatim());
        assertEquals(1, res.totalResults);
        assertEquals("foo", res.docs.get(0).getId());
    }

    @Test
    public void testNullField() throws Exception {
        Client cl = getDefaultClient();
        Schema sc = new Schema()
                .addTextField("title", 1.0)
                .addTextField("genre", 1.0)
                .addTextField("plot", 1.0)
                .addSortableNumericField("release_year")
                .addTagField("tag")
                .addGeoField("loc");
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        // create a document with a field set to null
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "another test with title ");
        fields.put("genre", "Comedy");
        fields.put("plot", "this is the plot for the test");
        fields.put("tag", "fun");
        fields.put("release_year", 2019);
        fields.put("loc", "-0.1,51.2");

        cl.addDocument("doc1", fields);
        SearchResult res = cl.search(new Query("title"));
        assertEquals(1, res.totalResults);

        fields = new HashMap<>();
        fields.put("title", "another title another test");
        fields.put("genre", "Action");
        fields.put("plot", null);
        fields.put("tag", null);

        try {
            cl.addDocument("doc2", fields);
            fail("Should throw a 'NullPointerException'.");
        } catch (NullPointerException e) {
            assertEquals("Document attribute 'tag' is null. (Remove it, or set a value)" , e.getMessage());
        }

        res = cl.search(new Query("title"));
        assertEquals(1, res.totalResults);

        // Testing with numerical value
        fields = new HashMap<>();
        fields.put("title", "another title another test");
        fields.put("genre", "Action");
        fields.put("release_year", null);
        try {
            cl.addDocument("doc2", fields);
            fail("Should throw a 'NullPointerException'.");
        } catch (NullPointerException e) {
            assertEquals("Document attribute 'release_year' is null. (Remove it, or set a value)" , e.getMessage());
        }
        res = cl.search(new Query("title"));
        assertEquals(1, res.totalResults);
    }


    @Test
    public void testDrop() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        for (int i = 0; i < 100; i++) {
            assertTrue(cl.addDocument(String.format("doc%d", i), fields));
        }

        SearchResult res = cl.search(new Query("hello world"));
        assertEquals(100, res.totalResults);

        assertTrue(cl.dropIndex());

        Jedis conn = cl.connection();

        Set<String> keys = conn.keys("*");
        assertTrue(keys.isEmpty());
    }
    
    @Test
    public void testAlterAdd() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        for (int i = 0; i < 100; i++) {
            assertTrue(cl.addDocument(String.format("doc%d", i), fields));
        }
        
        SearchResult res = cl.search(new Query("hello world"));
        assertEquals(100, res.totalResults);
        
        assertTrue(cl.alterIndex(new TagField("tags", ","), new TextField("name", 0.5)));
        for (int i = 0; i < 100; i++) {
          Map<String, Object> fields2 = new HashMap<>();
          fields2.put("name", "name" + i);
          fields2.put("tags", String.format("tagA,tagB,tag%d", i));
          assertTrue(cl.updateDocument(String.format("doc%d", i), 1.0, fields2));
        }
        SearchResult res2 = cl.search(new Query("@tags:{tagA}"));
        assertEquals(100, res2.totalResults);        
        
        Map<String, Object> info = cl.getInfo();
        assertEquals(TEST_INDEX, info.get("index_name"));
        assertEquals("tags",((List)((List)info.get("fields")).get(1)).get(0));
        assertEquals("TAG", ((List)((List)info.get("fields")).get(1)).get(2));
    }

    @Test
    public void testNoStem() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        Schema sc = new Schema().addTextField("stemmed", 1.0).addField(new Schema.TextField("notStemmed", 1.0, false, true));
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> doc = new HashMap<>();
        doc.put("stemmed", "located");
        doc.put("notStemmed", "located");
        // Store it
        assertTrue(cl.addDocument("doc", doc));

        // Query
        SearchResult res = cl.search(new Query("@stemmed:location"));
        assertEquals(1, res.totalResults);

        res = cl.search(new Query("@notStemmed:location"));
        assertEquals(0, res.totalResults);
    }
    
    @Test
    public void testPhoneticMatch() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        Schema sc = new Schema()
            .addTextField("noPhonetic", 1.0)
            .addField(new Schema.TextField("withPhonetic", 1.0, false, false, false, "dm:en"));
        
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> doc = new HashMap<>();
        doc.put("noPhonetic", "morfix");
        doc.put("withPhonetic", "morfix");
        
        // Store it
        assertTrue(cl.addDocument("doc", doc));

        // Query
        SearchResult res = cl.search(new Query("@withPhonetic:morphix=>{$phonetic:true}"));
        assertEquals(1, res.totalResults);

        try {
          cl.search(new Query("@noPhonetic:morphix=>{$phonetic:true}"));
          fail();
        }catch( JedisDataException e) {/*field does not support phonetics*/}       
        
        SearchResult res3 = cl.search(new Query("@withPhonetic:morphix=>{$phonetic:false}"));
        assertEquals(0, res3.totalResults);
    }

    @Test
    public void testInfo() throws Exception {
        Client cl = getDefaultClient();

        String MOVIE_ID = "movie_id";
        String TITLE = "title";
        String GENRE = "genre";
        String VOTES = "votes";
        String RATING = "rating";
        String RELEASE_YEAR = "release_year";
        String PLOT = "plot";
        String POSTER = "poster";

        Schema sc =  new Schema()
                    .addTextField(TITLE, 5.0)
                    .addSortableTextField(PLOT, 1.0)
                    .addSortableTagField(GENRE, ",")
                    .addSortableNumericField(RELEASE_YEAR)
                    .addSortableNumericField(RATING)
                    .addSortableNumericField(VOTES);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> info = cl.getInfo();
        assertEquals(TEST_INDEX, info.get("index_name"));

        assertEquals(6, ((List)info.get("fields")).size());
        assertEquals("global_idle", ((List)info.get("cursor_stats")).get(0));
        assertEquals(0L, ((List)info.get("cursor_stats")).get(1));
    }

    @Test
    public void testNoIndex() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema()
                .addField(new Schema.TextField("f1", 1.0, true, false, true))
                .addField(new Schema.TextField("f2", 1.0));
        cl.createIndex(sc, Client.IndexOptions.defaultOptions());

        Map<String, Object> mm = new HashMap<>();

        mm.put("f1", "MarkZZ");
        mm.put("f2", "MarkZZ");
        cl.addDocument("doc1", mm);

        mm.clear();
        mm.put("f1", "MarkAA");
        mm.put("f2", "MarkBB");
        cl.addDocument("doc2", mm);

        SearchResult res = cl.search(new Query("@f1:Mark*"));
        assertEquals(0, res.totalResults);

        res = cl.search(new Query("@f2:Mark*"));
        assertEquals(2, res.totalResults);

        Document[] docs = new Document[2];

        res = cl.search(new Query("@f2:Mark*").setSortBy("f1", false));
        assertEquals(2, res.totalResults);

        res.docs.toArray(docs);
        assertEquals("doc1", docs[0].getId());

        res = cl.search(new Query("@f2:Mark*").setSortBy("f1", true));
        res.docs.toArray(docs);
        assertEquals("doc2", docs[0].getId());

    }

    @Test
    public void testReplacePartial() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> mm = new HashMap<>();
        mm.put("f1", "f1_val");
        mm.put("f2", "f2_val");

        assertTrue(cl.addDocument("doc1", mm));
        assertTrue(cl.addDocument("doc2", mm));

        mm.clear();
        mm.put("f3", "f3_val");

        assertTrue(cl.updateDocument("doc1", 1.0, mm));
        assertTrue(cl.replaceDocument("doc2", 1.0, mm));

        // Search for f3 value. All documents should have it.
        SearchResult res = cl.search(new Query(("@f3:f3_Val")));
        assertEquals(2, res.totalResults);

        res = cl.search(new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        assertEquals(1, res.totalResults);
    }
    
    @Test
    public void testReplaceIf() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> mm = new HashMap<>();
        mm.put("f1", "v1_val");
        mm.put("f2", "v2_val");

        assertTrue(cl.addDocument("doc1", mm));
        assertTrue(cl.addDocument("doc2", mm));

        mm.clear();
        mm.put("f3", "v3_val");

        assertFalse(cl.updateDocument("doc1", 1.0, mm, "@f1=='vv1_val'"));
        // Search for f3 value. No documents should not have it.
        SearchResult res1 = cl.search(new Query(("@f3:f3_Val")));
        assertEquals(0, res1.totalResults);

        assertTrue(cl.updateDocument("doc1", 1.0, mm, "@f2=='v2_val'"));
        // Search for f3 value. All documents should have it.
        SearchResult res2 = cl.search(new Query(("@f3:v3_Val")));
        assertEquals(1, res2.totalResults);

        assertFalse(cl.replaceDocument("doc2", 1.0, mm, "@f1=='vv3_Val'"));

        // Search for f3 value. Only one document should have it.
        SearchResult res3 = cl.search(new Query(("@f3:v3_Val")));
        assertEquals(1, res3.totalResults);

        assertTrue(cl.replaceDocument("doc2", 1.0, mm, "@f1=='v1_val'"));

        // Search for f3 value. All documents should have it.
        SearchResult res4 = cl.search(new Query(("@f3:v3_Val")));
        assertEquals(2, res4.totalResults);
    }

    @Test
    public void testExplain() throws Exception {
        Client cl = getDefaultClient();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        cl.createIndex(sc, Client.IndexOptions.defaultOptions());

        String res = cl.explain(new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        assertNotNull(res);
        assertFalse(res.isEmpty());
    }

    @Test
    public void testHighlightSummarize() throws Exception {
        Client cl = getDefaultClient();
        Schema sc = new Schema().addTextField("text", 1.0);
        cl.createIndex(sc, Client.IndexOptions.defaultOptions());

        Map<String, Object> doc = new HashMap<>();
        doc.put("text", "Redis is often referred as a data structures server. What this means is that Redis provides access to mutable data structures via a set of commands, which are sent using a server-client model with TCP sockets and a simple protocol. So different processes can query and modify the same data structures in a shared way");
        // Add a document
        cl.addDocument("foo", 1.0, doc);
        Query q = new Query("data").highlightFields().summarizeFields();
        SearchResult res = cl.search(q);

        assertEquals("is often referred as a <b>data</b> structures server. What this means is that Redis provides... What this means is that Redis provides access to mutable <b>data</b> structures via a set of commands, which are sent using a... So different processes can query and modify the same <b>data</b> structures in a shared... ",
                res.docs.get(0).get("text"));
        
        q = new Query("data").highlightFields(new Query.HighlightTags("<u>", "</u>")).summarizeFields();
        res = cl.search(q);

        assertEquals("is often referred as a <u>data</u> structures server. What this means is that Redis provides... What this means is that Redis provides access to mutable <u>data</u> structures via a set of commands, which are sent using a... So different processes can query and modify the same <u>data</u> structures in a shared... ",
            res.docs.get(0).get("text"));

    }

    @Test
    public void testLanguage() throws Exception {
        Client cl = getDefaultClient();
        Schema sc = new Schema().addTextField("text", 1.0);
        cl.createIndex(sc, Client.IndexOptions.defaultOptions());

        Document d = new Document("doc1").set("text", "hello");
        AddOptions options = new AddOptions().setLanguage("spanish");
        assertTrue(cl.addDocument(d, options));
        boolean caught = false;

        options.setLanguage("ybreski");
        cl.deleteDocument(d.getId());

        try {
            cl.addDocument(d, options);
        } catch (JedisDataException t) {
            caught = true;
        }
        assertTrue(caught);
    }

    @Test
    public void testDropMissing() throws Exception {
        Client cl = createClient("dummyIndexNotExist");
        assertFalse(cl.dropIndex(true));
        boolean caught = false;
        try {
            cl.dropIndex();
        } catch (JedisDataException ex) {
            caught = true;
        }
        assertTrue(caught);
    }

    @Test
    public void testGet() throws Exception {
        Client cl = getDefaultClient();
        cl.createIndex(new Schema().addTextField("txt1", 1.0), Client.IndexOptions.defaultOptions());
        cl.addDocument(new Document("doc1").set("txt1", "Hello World!"), new AddOptions());
        Document d = cl.getDocument("doc1");
        assertNotNull(d);
        assertEquals("Hello World!", d.get("txt1"));

        // Get something that does not exist. Shouldn't explode
        assertNull(cl.getDocument("nonexist"));
        
        // Test decode=false mode
        d = cl.getDocument("doc1", false);
        assertNotNull(d);
        assertTrue(Arrays.equals( SafeEncoder.encode("Hello World!"), (byte[])d.get("txt1")));
    }
    
    @Test
    public void testMGet() throws Exception {
        Client cl = getDefaultClient();
        cl.createIndex(new Schema().addTextField("txt1", 1.0), Client.IndexOptions.defaultOptions());
        cl.addDocument(new Document("doc1").set("txt1", "Hello World!1"), new AddOptions());
        cl.addDocument(new Document("doc2").set("txt1", "Hello World!2"), new AddOptions());
        cl.addDocument(new Document("doc3").set("txt1", "Hello World!3"), new AddOptions());
       
        List<Document> docs = cl.getDocuments();
        assertEquals(0, docs.size());
        
        docs = cl.getDocuments("doc1", "doc3", "doc4");
        assertEquals(3, docs.size());
        assertEquals("Hello World!1", docs.get(0).get("txt1"));
        assertEquals("Hello World!3", docs.get(1).get("txt1"));
        assertNull(docs.get(2));

        // Test decode=false mode
        docs = cl.getDocuments(false, "doc2");
        assertEquals(1, docs.size());
        assertTrue(Arrays.equals( SafeEncoder.encode("Hello World!2"), (byte[])docs.get(0).get("txt1")));
    }


    @Test
    public void testAddSuggestionGetSuggestionFuzzy() throws Exception {
        Client cl = getDefaultClient();
        Suggestion suggestion = Suggestion.builder().str("TOPIC OF WORDS").score(1).build();
        // test can add a suggestion string
        assertTrue(suggestion.toString() + " insert should of returned at least 1", cl.addSuggestion(suggestion, true) > 0);
        // test that the partial part of that string will be returned using fuzzy

        assertEquals(suggestion.toString() + " suppose to be returned", suggestion, cl.getSuggestion(suggestion.getString().substring(0, 3), SuggestionOptions.builder().build()).get(0));
    }


    @Test
    public void testAddSuggestionGetSuggestion() throws Exception {
        Client cl = getDefaultClient();
        
        try {
          Suggestion.builder().str("ANOTHER_WORD").score(3).build();
          fail("Illegal score");
        } catch(IllegalStateException e) {}
        
        try {
          Suggestion.builder().score(1).build();
          fail("Missing required string");
        } catch(IllegalStateException e) {}
        
        Suggestion suggestion = Suggestion.builder().str("ANOTHER_WORD").score(1).build();
        Suggestion noMatch = Suggestion.builder().str("_WORD MISSED").score(1).build();

        assertTrue(suggestion.toString() + " should of inserted at least 1", cl.addSuggestion(suggestion, false) > 0);
        assertTrue(noMatch.toString() + " should of inserted at least 1", cl.addSuggestion(noMatch, false) > 0);

        // test that with a partial part of that string will have the entire word returned SuggestionOptions.builder().build()
        assertEquals(suggestion.getString() + " did not get a match with 3 characters", 1, cl.getSuggestion(suggestion.getString().substring(0, 3), SuggestionOptions.builder().fuzzy().build()).size());

        // turn off fuzzy start at second word no hit
        assertEquals(noMatch.getString() + " no fuzzy and starting at 1, should not match", 0, cl.getSuggestion(noMatch.getString().substring(1, 6), SuggestionOptions.builder().build()).size());
        // my attempt to trigger the fuzzy by 1 character
        assertEquals(noMatch.getString() + " fuzzy is on starting at 1 position should match", 1, cl.getSuggestion(noMatch.getString().substring(1, 6), SuggestionOptions.builder().fuzzy().build()).size());
    }


    @Test
    public void testAddSuggestionGetSuggestionPayloadScores() throws Exception {
        Client cl = getDefaultClient();

        Suggestion suggestion = Suggestion.builder().str("COUNT_ME TOO").payload("PAYLOADS ROCK ").score(0.2).build();
        assertTrue(suggestion.toString() + " insert should of at least returned 1", cl.addSuggestion(suggestion, false) > 0);
        assertTrue("Count single added should return more than 1", cl.addSuggestion(suggestion.toBuilder().str("COUNT").payload("My PAYLOAD is better").build(), false) > 1);
        assertTrue("Count single added should return more than 1", cl.addSuggestion(suggestion.toBuilder().str("COUNT_ANOTHER").score(1).payload(null).build(), false) > 1);

        Suggestion noScoreOrPayload = Suggestion.builder().str("COUNT NO PAYLOAD OR COUNT").build();
        assertTrue("Count single added should return more than 1", cl.addSuggestion(noScoreOrPayload, true) > 1);


        List<Suggestion> payloads = cl.getSuggestion(suggestion.getString().substring(0, 3), SuggestionOptions.builder().with(SuggestionOptions.With.PAYLOAD_AND_SCORES).build());
        assertEquals("4 suggestions with scores and payloads ", 4, payloads.size());
        assertTrue("Assert that a suggestion has a payload ", payloads.get(2).getPayload().length() > 0);
        assertTrue("Assert that a suggestion has a score not default 1 ", payloads.get(1).getScore() < .299);


    }


    @Test
    public void testAddSuggestionGetSuggestionPayload() throws Exception {
        Client cl = getDefaultClient();
        cl.addSuggestion(Suggestion.builder().str("COUNT_ME TOO").payload("PAYLOADS ROCK ").build(), false);
        cl.addSuggestion(Suggestion.builder().str("COUNT").payload("ANOTHER PAYLOAD ").build(), false);
        cl.addSuggestion(Suggestion.builder().str("COUNTNO PAYLOAD OR COUNT").build(), false);

        // test that with a partial part of that string will have the entire word returned
        List<Suggestion> payloads = cl.getSuggestion("COU", SuggestionOptions.builder().max(3).fuzzy().with(SuggestionOptions.With.PAYLOAD).build());
        assertEquals("3 suggestions payloads ", 3, payloads.size());

    }


    @Test
    public void testGetSuggestionNoPayloadTwoOnly() throws Exception {
        Client cl = getDefaultClient();

        cl.addSuggestion(Suggestion.builder().str("DIFF_WORD").score(0.4).payload("PAYLOADS ROCK ").build(), false);
        cl.addSuggestion(Suggestion.builder().str("DIFF wording").score(0.5).payload("ANOTHER PAYLOAD ").build(), false);
        cl.addSuggestion(Suggestion.builder().str("DIFFERENT").score(0.7).payload("I am a payload").build(), false);

        List<Suggestion> payloads = cl.getSuggestion("DIF", SuggestionOptions.builder().max(2).build());
        assertEquals("3 suggestions should match but only asking for 2 and payloads should have 2 items in array", 2, payloads.size());

        List<Suggestion> three = cl.getSuggestion("DIF", SuggestionOptions.builder().max(3).build());
        assertEquals("3 suggestions and payloads should have 3 items in array", 3, three.size());

    }

    @Test
    public void testGetSuggestionWithScore() throws Exception {
        Client cl = getDefaultClient();

        cl.addSuggestion(Suggestion.builder().str("DIFF_WORD").score(0.4).payload("PAYLOADS ROCK ").build(), true);
        List<Suggestion> list = cl.getSuggestion("DIF", SuggestionOptions.builder().max(2).with(SuggestionOptions.With.SCORES).build());
        assertTrue(list.get(0).getScore() <= .2);

    }

    @Test
    public void testGetSuggestionAllNoHit() throws Exception {
        Client cl = getDefaultClient();

        cl.addSuggestion(Suggestion.builder().str("NO WORD").score(0.4).build(), false);

        List<Suggestion> none = cl.getSuggestion("DIF", SuggestionOptions.builder().max(3).with(SuggestionOptions.With.SCORES).build());
        assertEquals("Empty list not hit in index for partial word", 0, none.size());

    }

    @Test
    public void testAddSuggestionDeleteSuggestionLength() throws Exception {
        Client cl = getDefaultClient();
        cl.addSuggestion(Suggestion.builder().str("TOPIC OF WORDS").score(1).build(), true);
        cl.addSuggestion(Suggestion.builder().str("ANOTHER ENTRY").score(1).build(), true);

        long result = cl.deleteSuggestion("ANOTHER ENTRY");
        assertEquals("The delete of the suggestion should return 1", 1, result);
        assertEquals(1L, cl.getSuggestionLength().longValue());

        result = cl.deleteSuggestion("ANOTHER ENTRY THAT IS NOT PRESENT");
        assertEquals("The delete of the suggestion should return 0", 0, result);
        assertEquals(1L, cl.getSuggestionLength().longValue());
    }

    @Test
    public void testAddSuggestionGetSuggestionLength() throws Exception {
        Client cl = getDefaultClient();
        cl.addSuggestion(Suggestion.builder().str("TOPIC OF WORDS").score(1).build(), true);
        cl.addSuggestion(Suggestion.builder().str("ANOTHER ENTRY").score(1).build(), true);
        assertEquals(2L, cl.getSuggestionLength().longValue());

        cl.addSuggestion(Suggestion.builder().str("FINAL ENTRY").score(1).build(), true);
        assertEquals(3L, cl.getSuggestionLength().longValue());
    }


    @Test
    public void testGetTagField() {
        Client cl = getDefaultClient();
        Schema sc = new Schema()
                .addTextField("title", 1.0)
                .addTagField("category");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields1 = new HashMap<>();
        fields1.put("title", "hello world");
        fields1.put("category", "red");
        assertTrue(cl.addDocument("foo", fields1));
        Map<String, Object> fields2 = new HashMap<>();
        fields2.put("title", "hello world");
        fields2.put("category", "blue");
        assertTrue(cl.addDocument("bar", fields2));
        Map<String, Object> fields3 = new HashMap<>();
        fields3.put("title", "hello world");
        fields3.put("category", "green,yellow");
        assertTrue(cl.addDocument("baz", fields3));
        Map<String, Object> fields4 = new HashMap<>();
        fields4.put("title", "hello world");
        fields4.put("category", "orange;purple");
        assertTrue(cl.addDocument("qux", fields4));

        assertEquals(1, cl.search(new Query("@category:{red}")).totalResults);
        assertEquals(1, cl.search(new Query("@category:{blue}")).totalResults);
        assertEquals(1, cl.search(new Query("hello @category:{red}")).totalResults);
        assertEquals(1, cl.search(new Query("hello @category:{blue}")).totalResults);
        assertEquals(1, cl.search(new Query("@category:{yellow}")).totalResults);
        assertEquals(0, cl.search(new Query("@category:{purple}")).totalResults);
        assertEquals(1, cl.search(new Query("@category:{orange\\;purple}")).totalResults);
        assertEquals(4, cl.search(new Query("hello")).totalResults);
    }

    @Test
    public void testGetTagFieldWithNonDefaultSeparator() {
        Client cl = getDefaultClient();
        Schema sc = new Schema()
                .addTextField("title", 1.0)
                .addTagField("category", ";");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> fields1 = new HashMap<>();
        fields1.put("title", "hello world");
        fields1.put("category", "red");
        assertTrue(cl.addDocument("foo", fields1));
        Map<String, Object> fields2 = new HashMap<>();
        fields2.put("title", "hello world");
        fields2.put("category", "blue");
        assertTrue(cl.addDocument("bar", fields2));
        Map<String, Object> fields3 = new HashMap<>();
        fields3.put("title", "hello world");
        fields3.put("category", "green;yellow");
        assertTrue(cl.addDocument("baz", fields3));
        Map<String, Object> fields4 = new HashMap<>();
        fields4.put("title", "hello world");
        fields4.put("category", "orange,purple");
        assertTrue(cl.addDocument("qux", fields4));

        assertEquals(1, cl.search(new Query("@category:{red}")).totalResults);
        assertEquals(1, cl.search(new Query("@category:{blue}")).totalResults);
        assertEquals(1, cl.search(new Query("hello @category:{red}")).totalResults);
        assertEquals(1, cl.search(new Query("hello @category:{blue}")).totalResults);
        assertEquals(1, cl.search(new Query("hello @category:{yellow}")).totalResults);
        assertEquals(0, cl.search(new Query("@category:{purple}")).totalResults);
        assertEquals(1, cl.search(new Query("@category:{orange\\,purple}")).totalResults);
        assertEquals(4, cl.search(new Query("hello")).totalResults);
    }
    
    @Test
    public void testMultiDocuments() {
    	 Client cl = getDefaultClient();
         Schema sc = new Schema().addTextField("title", 1.0).addTextField("body", 1.0);
         
         assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
         
         Map<String, Object> fields = new HashMap<>();
         fields.put("title", "hello world");
         fields.put("body", "lorem ipsum");

         boolean[] results = cl.addDocuments(new Document("doc1",fields), new Document("doc2",fields), new Document("doc3",fields));
         
         assertArrayEquals(new boolean[]{true, true, true}, results);   
         
         assertEquals(3, cl.search(new Query("hello world")).totalResults);
         
         results = cl.addDocuments(new Document("doc4",fields), new Document("doc2",fields), new Document("doc5",fields));
         assertArrayEquals(new boolean[]{true, false, true}, results);   
         
         results = cl.deleteDocuments(true, "doc1", "doc2", "doc36");
         assertArrayEquals(new boolean[]{true, true, false}, results);   
    }
    
    @Test
    public void testReturnFields() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        Schema sc = new Schema().addTextField("field1", 1.0).addTextField("field2", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));


        Map<String, Object> doc = new HashMap<>();
        doc.put("field1", "value1");
        doc.put("field2", "value2");
        // Store it
        assertTrue(cl.addDocument("doc", doc));

        // Query
        SearchResult res = cl.search(new Query("*").returnFields("field1"));
        assertEquals(1, res.totalResults);
        assertEquals("value1", res.docs.get(0).get("field1"));
        assertEquals(null, res.docs.get(0).get("field2"));
    }
    
    @Test
    public void testInKeys() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        Schema sc = new Schema().addTextField("field1", 1.0).addTextField("field2", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        Map<String, Object> doc = new HashMap<>();
        doc.put("field1", "value");
        doc.put("field2", "not");
        
        // Store it
        assertTrue(cl.addDocument("doc1", doc));
        assertTrue(cl.addDocument("doc2", doc));
        
        // Query
        SearchResult res = cl.search(new Query("value").limitKeys("doc1"));
        assertEquals(1, res.totalResults);
        assertEquals("doc1", res.docs.get(0).getId());
        assertEquals("value", res.docs.get(0).get("field1"));
        assertEquals(null, res.docs.get(0).get("value"));
    }

    @Test
    public void testBlobField() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        Schema sc = new Schema().addTextField("field1", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        byte[] blob = new byte[] {1,2,3,4,5,6,7,8,9,10,11, 12};
        
        Map<String, Object> doc = new HashMap<>();
        doc.put("field1", "value");
        doc.put("field2", blob);
        
        // Store it
        assertTrue(cl.addDocument("doc1", doc));
        
        // Query
        SearchResult res = cl.search(new Query("value"), false);
        assertEquals(1, res.totalResults);
        assertEquals("doc1", res.docs.get(0).getId());
        assertEquals("value", res.docs.get(0).getString("field1"));
        assertTrue( Arrays.equals( blob, (byte[])res.docs.get(0).get("field2")));
    }

    @Test
    public void testConfig() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();

        boolean result = cl.setConfig(ConfigOption.TIMEOUT, "100");
        assertTrue(result);
        Map<String, String> configMap = cl.getAllConfig();
        assertEquals("100", configMap.get(ConfigOption.TIMEOUT.name()));
        assertEquals("100", cl.getConfig(ConfigOption.TIMEOUT));

        cl.setConfig(ConfigOption.ON_TIMEOUT, "fail");
        assertEquals("fail", cl.getConfig(ConfigOption.ON_TIMEOUT));

        try {
            assertFalse( cl.setConfig(ConfigOption.ON_TIMEOUT, "null"));
        }catch(JedisDataException e) {
            // Should throw an exception after RediSearch 2.2
        }
    }

    @Test
    public void testAlias() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();

        Schema sc = new Schema().addTextField("field1", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));
        Map<String, Object> doc = new HashMap<>();
        doc.put("field1", "value");
        assertTrue(cl.addDocument("doc1", doc));

        assertTrue(cl.addAlias("ALIAS1"));
        Client alias1 = createClient("ALIAS1");
        SearchResult res1 = alias1.search(new Query("*").returnFields("field1"));
        assertEquals(1, res1.totalResults);
        assertEquals("value", res1.docs.get(0).get("field1"));

        assertTrue(cl.updateAlias("ALIAS2"));
        Client alias2 = createClient("ALIAS2");
        SearchResult res2 = alias2.search(new Query("*").returnFields("field1"));
        assertEquals(1, res2.totalResults);
        assertEquals("value", res2.docs.get(0).get("field1"));

        try {
            cl.deleteAlias("ALIAS3");
            fail("Should throw JedisDataException");
        } catch (JedisDataException e) {
            // Alias does not exist
        }
        assertTrue(cl.deleteAlias("ALIAS2"));
        try {
            cl.deleteAlias("ALIAS2");
            fail("Should throw JedisDataException");
        } catch (JedisDataException e) {
            // Alias does not exist
        }
    }
  
    @Test
    public void testSyn() throws Exception {
        Client cl = getDefaultClient();
        cl.connection().flushDB();
        
        Schema sc = new Schema().addTextField("name", 1.0).addTextField("addr", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.defaultOptions()));

        long group1, group2;
        try {
          group1 = cl.addSynonym("girl", "baby");
          assertTrue(cl.updateSynonym(group1, "child"));
          group2 = cl.addSynonym("child");
          assertNotSame(group1, group2);
        }catch(JedisDataException e) {
          // TF.SYNADD is not supported since RediSearch 2.0
          assertEquals( "No longer suppoted, use FT.SYNUPDATE", e.getMessage());
          
          group1 = 345L;
          group2 = 789L;
          assertTrue(cl.updateSynonym(group1, "girl", "baby"));
          assertTrue(cl.updateSynonym(group1, "child"));
          assertTrue(cl.updateSynonym(group2, "child"));
        }
        
        Map<String, List<String>> dump = cl.dumpSynonym();
        
        Map<String, List<String>> expected = new HashMap<>();
        expected.put("girl", Arrays.asList(String.valueOf(group1)));
        expected.put("baby", Arrays.asList(String.valueOf(group1)));
        expected.put("child", Arrays.asList(String.valueOf(group1), String.valueOf(group2)));
        assertEquals(expected, dump);               
    }
}
