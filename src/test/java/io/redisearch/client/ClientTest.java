package io.redisearch.client;

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchClient;
import io.redisearch.SearchResult;
import io.redisearch.Suggestion;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;


/**
 * Created by dvirsky on 09/02/17.
 */
public class ClientTest {
    // NOTE: My IDEA config hard-codes this property to 7777! - don't modify this line, rather change it in the
    // configuration settings
    static private final int TEST_PORT = Integer.parseInt(System.getProperty("redis.port", "6379"));
    static private final String TEST_HOST = System.getProperty("redis.host", "localhost");
    static private final String TEST_INDEX = System.getProperty("redis.rsIndex", "testung");

    protected SearchClient getClient(String indexName) {
        return ClientBuilder.builder().indexName(indexName).host(TEST_HOST).port(TEST_PORT).build();
    }

    protected SearchClient getClient() {
        return getClient(TEST_INDEX);
    }

    @Before
    public void setUp() {
        ((Client) getClient())._conn().flushDB();
    }

    @Test
    public void search() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0).addTextField("body", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
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
            //System.out.println(d);
        }

        assertTrue(cl.deleteDocument("doc0"));
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
    public void testNumericFilter() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0).addNumericField("price");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
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
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0);


        assertTrue(cl.createIndex(sc,
                Client.IndexOptions.Default().SetStopwords("foo", "bar", "baz")));

        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world foo bar");
        assertTrue(cl.addDocument("doc1", fields));
        SearchResult res = cl.search(new Query("hello world"));
        assertEquals(1, res.totalResults);
        res = cl.search(new Query("foo bar"));
        assertEquals(0, res.totalResults);

        ((Client)cl)._conn().flushDB();

        assertTrue(cl.createIndex(sc,
                Client.IndexOptions.Default().SetNoStopwords()));
        fields.put("title", "hello world foo bar to be or not to be");
        assertTrue(cl.addDocument("doc1", fields));

        assertEquals(1, cl.search(new Query("hello world")).totalResults);
        assertEquals(1, cl.search(new Query("foo bar")).totalResults);
        assertEquals(1, cl.search(new Query("to be or not to be")).totalResults);
    }


    @Test
    public void testGeoFilter() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0).addGeoField("loc");

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
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

    @Test
    public void testPayloads() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));

        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        String payload = "foo bar";
        assertTrue(cl.addDocument("doc1", 1.0, fields, false, false, payload.getBytes()));
        SearchResult res = cl.search(new Query("hello world").setWithPaload());
        assertEquals(1, res.totalResults);
        assertEquals(1, res.docs.size());

        assertEquals(payload, new String(res.docs.get(0).getPayload()));
    }

    @Test
    public void testQueryFlags() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
        Map<String, Object> fields = new HashMap<>();


        for (int i = 0; i < 100; i++) {
            fields.put("title", i % 2 == 1 ? "hello worlds" : "hello world");
            assertTrue(cl.addDocument(String.format("doc%d", i), (double) i / 100.0, fields));
        }

        Query q = new Query("hello").setWithScores();
        SearchResult res = cl.search(q);

        assertEquals(100, res.totalResults);
        assertEquals(10, res.docs.size());

        for (Document d : res.docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertTrue(d.getScore() != 1.0);
            assertTrue(((String) d.get("title")).startsWith("hello world"));
        }

        q = new Query("hello").setNoContent();
        res = cl.search(q);
        for (Document d : res.docs) {
            assertTrue(d.getId().startsWith("doc"));
            assertTrue(d.getScore() == 1.0);
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
        SearchClient cl = getClient();
        Schema sc = new Schema().addSortableTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
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
        SearchClient cl = getClient();
        Jedis conn = ((Client)cl)._conn();
        Schema sc = new Schema().addTextField("title", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
        HashMap hm = new HashMap();
        hm.put("title", "hello world");
        conn.hmset("foo", hm);

        assertTrue(cl.addHash("foo", 1, false));
        SearchResult res = cl.search(new Query("hello world").setVerbatim());
        assertEquals(1, res.totalResults);
        assertEquals("foo", res.docs.get(0).getId());
    }

    @Test
    public void testDrop() throws Exception {
        SearchClient cl = getClient();
        ((Client)cl)._conn().flushDB();

        Schema sc = new Schema().addTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
        Map<String, Object> fields = new HashMap<>();
        fields.put("title", "hello world");
        for (int i = 0; i < 100; i++) {
            assertTrue(cl.addDocument(String.format("doc%d", i), fields));
        }

        SearchResult res = cl.search(new Query("hello world"));
        assertEquals(100, res.totalResults);

        assertTrue(cl.dropIndex());

        Jedis conn = ((Client)cl)._conn();

        Set<String> keys = conn.keys("*");
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testNoStem() throws Exception {
        SearchClient cl = getClient();
        ((Client)cl)._conn().flushDB();
        Schema sc = new Schema().addTextField("stemmed", 1.0).addField(new Schema.TextField("notStemmed", 1.0, false, true));
        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));

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
    public void testInfo() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema().addTextField("title", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));

        Map<String, Object> info = cl.getInfo();
        assertEquals(TEST_INDEX, info.get("index_name"));

    }

    @Test
    public void testNoIndex() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema()
                .addField(new Schema.TextField("f1", 1.0, true, false, true))
                .addField(new Schema.TextField("f2", 1.0));
        cl.createIndex(sc, Client.IndexOptions.Default());

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
        SearchClient cl = getClient();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

        Map<String, Object> mm = new HashMap<>();
        mm.put("f1", "f1_val");
        mm.put("f2", "f2_val");

        cl.addDocument("doc1", mm);
        cl.addDocument("doc2", mm);

        mm.clear();
        mm.put("f3", "f3_val");

        cl.updateDocument("doc1", 1.0, mm);
        cl.replaceDocument("doc2", 1.0, mm);

        // Search for f3 value. All documents should have it.
        SearchResult res = cl.search(new Query(("@f3:f3_Val")));
        assertEquals(2, res.totalResults);

        res = cl.search(new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        assertEquals(1, res.totalResults);
    }

    @Test
    public void testExplain() throws Exception {
        SearchClient cl = getClient();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

        String res = cl.explain(new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        assertNotNull(res);
        assertFalse(res.isEmpty());
    }

    @Test
    public void testHighlightSummarize() throws Exception {
        SearchClient cl = getClient();
        Schema sc = new Schema().addTextField("text", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

        Map<String, Object> doc = new HashMap<>();
        doc.put("text", "Redis is often referred as a data structures server. What this means is that Redis provides access to mutable data structures via a set of commands, which are sent using a server-client model with TCP sockets and a simple protocol. So different processes can query and modify the same data structures in a shared way");
        // Add a document
        cl.addDocument("foo", 1.0, doc);
        Query q = new Query("data").highlightFields().summarizeFields();
        SearchResult res = cl.search(q);

        assertEquals("is often referred as a <b>data</b> structures server. What this means is that Redis provides... What this means is that Redis provides access to mutable <b>data</b> structures via a set of commands, which are sent using a... So different processes can query and modify the same <b>data</b> structures in a shared... ",
                res.docs.get(0).get("text"));
    }

    @Test
    public void testLanguage() throws Exception {
        SearchClient cl = getClient();
        Schema sc = new Schema().addTextField("text", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

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
        SearchClient cl = getClient("dummyIndexNotExist");
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
        SearchClient cl = getClient();
        cl.createIndex(new Schema().addTextField("txt1", 1.0), Client.IndexOptions.Default());
        cl.addDocument(new Document("doc1").set("txt1", "Hello World!"), new AddOptions());
        Document d = cl.getDocument("doc1");
        assertNotNull(d);
        assertEquals("Hello World!", d.get("txt1"));

        // Get something that does not exist. Shouldn't explode
        assertNull(cl.getDocument("nonexist"));
    }

    @Test
    public void testAddSuggestionGetSuggestionFuzzy() throws Exception {
        SearchClient cl = getClient();
        Suggestion suggestion = Suggestion.builder().str("TOPIC OF WORDS").score(10).build();
        // test can add a suggestion string
        assertTrue(suggestion.toString() + " insert should of returned at least 1", cl.addSuggestion(suggestion, true) > 0);
        // test that the partial part of that string will be returned using fuzzy
        assertTrue(suggestion.toString() + " suppose to be returned", cl.getSuggestion(suggestion.getString().substring(0, 3), false, 6, true, false).contains(suggestion.getString()));
    }

    @Test
    public void testAddSuggestionGetSuggestion() throws Exception {
        SearchClient cl = getClient();
        Suggestion suggestion = Suggestion.builder().str("ANOTHER_WORD").score(10).build();
        Suggestion noMatch = Suggestion.builder().str("_WORD MISSED").score(10).build();

        assertTrue(suggestion.toString() + " should of inserted at least 1", cl.addSuggestion(suggestion, false) > 0);
        assertTrue(noMatch.toString() + " should of inserted at least 1", cl.addSuggestion(noMatch, false) > 0);

        // test that with a partial part of that string will have the entire word returned
        assertTrue(suggestion.getString() + " did not get a match with 3 characters", cl.getSuggestion(suggestion.getString().substring(0, 3), false, 6, false, false).contains(suggestion.getString()));
        // turn off fuzzy start at second word no hit
        assertFalse(noMatch.getString() + " no fuzzy and starting at 1, should not match", cl.getSuggestion(noMatch.getString().substring(1, 6), false, 6, false, false).contains(noMatch.getString()));
        // my attempt to trigger the fuzzy and not
        assertTrue(noMatch.getString() + " fuzzy is on starting at 1 position should match", cl.getSuggestion(noMatch.getString().substring(1, 6), false, 6, true, false).contains(noMatch.getString()));
    }

    @Test
    public void testAddSuggestionGetSuggestionPayloadScores() throws Exception {
        SearchClient cl = getClient();
        Suggestion suggestion = Suggestion.builder().str("COUNT_ME TOO").payload("PAYLOADS ROCK ".getBytes()).score(8).build();
        assertTrue(suggestion.toString() + " insert should of at least returned 1", cl.addSuggestion(suggestion, false) > 0);
        assertTrue("Count single added should return more than 1", cl.addSuggestion(suggestion.toBuilder().str("COUNT").payload("My PAYLOAD is better".getBytes()).build(), false) > 1);
        // test that with a partial part of that string will have the entire word returned
        List<String> payloads = cl.getSuggestion(suggestion.getString().substring(0, 3), true, 6, false, true);
        assertEquals("2 suggestions with scores and payloads should have 6 items in array", 6, payloads.size());

    }


}