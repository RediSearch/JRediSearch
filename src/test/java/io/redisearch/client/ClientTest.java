package io.redisearch.client;

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.*;


/**
 * Created by dvirsky on 09/02/17.
 */
public class ClientTest {
    @Test
    public void search() throws Exception {
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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

        cl._conn().flushDB();

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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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

        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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
        assertEquals(100, res.totalResults);
        res = cl.search(new Query("hello a world").setVerbatim().setNoStopwords());
        assertEquals(0, res.totalResults);
    }

    @Test
    public void testSortQueryFlags() throws Exception {

        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

        Schema sc = new Schema().addSortableTextField("title", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));
        Map<String, Object> fields = new HashMap<>();

        fields.put("title", "b title");
        cl.addDocument("doc1", 1.0 , fields,false,true,null);

        fields.put("title", "a title");
        cl.addDocument("doc2", 1.0, fields ,false,true,null);

        fields.put("title", "c title");
        cl.addDocument("doc3", 1.0 , fields,false,true,null);

        Query q = new Query("title").setSortBy("title",true);
        SearchResult res = cl.search(q);

        assertEquals(3, res.totalResults);
        Document doc1 =  res.docs.get(0);
        assertEquals("a title",doc1.get("title") );

        doc1 =  res.docs.get(1);
        assertEquals("b title",doc1.get("title") );

        doc1 =  res.docs.get(2);
        assertEquals("c title",doc1.get("title") );
    }

    @Test
    public void testAddHash() throws Exception {
        Client cl = new Client("testung", "localhost", 6379);
        Jedis conn = cl._conn();
        conn.flushDB();
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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

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

        Jedis conn = cl._conn();

        Set<String> keys = conn.keys("*");
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testNoStem() throws Exception {
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();
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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

        Schema sc = new Schema().addTextField("title", 1.0);
        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));

        Map<String, Object> info = cl.getInfo();
        assertEquals("testung", info.get("index_name"));

    }

    @Test
    public void testNoIndex() throws Exception {
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

        Schema sc = new Schema()
                .addField(new Schema.TextField("f1", 1.0,true, false, true))
                .addField(new Schema.TextField("f2", 1.0));
        cl.createIndex(sc, Client.IndexOptions.Default());

        Map<String,Object> mm = new HashMap<>();

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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

        Map<String,Object> mm = new HashMap<>();
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
        Client cl = new Client("testung", "localhost", 6379);
        cl._conn().flushDB();

        Schema sc = new Schema()
                .addTextField("f1", 1.0)
                .addTextField("f2", 1.0)
                .addTextField("f3", 1.0);
        cl.createIndex(sc, Client.IndexOptions.Default());

        String res = cl.explain(new Query("@f3:f3_val @f2:f2_val @f1:f1_val"));
        assertNotNull(res);
        assertFalse(res.isEmpty());
    }
}