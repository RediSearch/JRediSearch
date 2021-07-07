package io.redisearch.client;

import io.redisearch.*;
import io.redisearch.Schema.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;

import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClientJsonTest extends TestBase {

    public static final String JSON_ROOT = "$";

    private enum JsonCommands implements ProtocolCommand {

        SET;

        @Override
        public byte[] getRaw() {
            return ("JSON." + name()).getBytes();
        }
    }

    @BeforeClass
    public static void prepare() {
        TEST_INDEX = "json-index";
        TestBase.prepare();
    }

    @AfterClass
    public static void tearDown() {
        TestBase.tearDown();
    }

    private static void setJson(Jedis jedis, String key, JSONObject json) {
        jedis.sendCommand(JsonCommands.SET, key, JSON_ROOT, json.toString());
    }

    private static JSONObject toJson(Object... values) {
        JSONObject json = new JSONObject();
        for (int i = 0; i < values.length; i += 2) {
            json.put((String) values[i], values[i + 1]);
        }
        return json;
    }

    @Test
    public void create() {
        Schema schema = new Schema().addTextField("$.first", 1.0).addTextField("$.last", 1.0)
                .addNumericField("$.age");
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON)
                .setPrefixes(new String[]{"student:", "pupil:"});

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        try (Jedis jedis = search.connection()) {
            setJson(jedis, "profesor:5555", toJson("first", "Albert", "last", "Blue", "age", 55));
            setJson(jedis, "student:1111", toJson("first", "Joe", "last", "Dod", "age", 18));
            setJson(jedis, "pupil:2222", toJson("first", "Jen", "last", "Rod", "age", 14));
            setJson(jedis, "student:3333", toJson("first", "El", "last", "Mark", "age", 17));
            setJson(jedis, "pupil:4444", toJson("first", "Pat", "last", "Shu", "age", 21));
            setJson(jedis, "student:5555", toJson("first", "Joen", "last", "Ko", "age", 20));
            setJson(jedis, "teacher:6666", toJson("first", "Pat", "last", "Rod", "age", 20));
        }

        SearchResult noFilters = search.search(new Query());
        assertEquals(5, noFilters.totalResults);

        SearchResult res1 = search.search(new Query("@\\$\\.first:Jo*"));
        assertEquals(2, res1.totalResults);

        SearchResult res2 = search.search(new Query("@\\$\\.first:Pat"));
        assertEquals(1, res2.totalResults);
    }

    @Test
    public void createWithFieldNames() {
        Schema schema = new Schema()
                .addField(new TextField(FieldName.of("$.first").as("first")))
                .addField(new TextField(FieldName.of("$.last")))
                .addField(new Field(FieldName.of("$.age").as("age"), FieldType.Numeric));
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON)
                .setPrefixes(new String[]{"student:", "pupil:"});

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        try (Jedis jedis = search.connection()) {
            setJson(jedis, "profesor:5555", toJson("first", "Albert", "last", "Blue", "age", 55));
            setJson(jedis, "student:1111", toJson("first", "Joe", "last", "Dod", "age", 18));
            setJson(jedis, "pupil:2222", toJson("first", "Jen", "last", "Rod", "age", 14));
            setJson(jedis, "student:3333", toJson("first", "El", "last", "Mark", "age", 17));
            setJson(jedis, "pupil:4444", toJson("first", "Pat", "last", "Shu", "age", 21));
            setJson(jedis, "student:5555", toJson("first", "Joen", "last", "Ko", "age", 20));
            setJson(jedis, "teacher:6666", toJson("first", "Pat", "last", "Rod", "age", 20));
        }

        SearchResult noFilters = search.search(new Query());
        assertEquals(5, noFilters.totalResults);

        SearchResult asOriginal = search.search(new Query("@\\$\\.first:Jo*"));
        assertEquals(0, asOriginal.totalResults);

        SearchResult asAttribute = search.search(new Query("@first:Jo*"));
        assertEquals(2, asAttribute.totalResults);

        SearchResult nonAttribute = search.search(new Query("@\\$\\.last:Rod"));
        assertEquals(1, nonAttribute.totalResults);
    }

    @Test
    public void parseJson() {
        Schema schema = new Schema();
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON);

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        String id = "student:1111";
        JSONObject json = toJson("first", "Joe", "last", "Dod", "age", 18);
        try (Jedis jedis = search.connection()) {
            setJson(jedis, id, json);
        }

        // query
        SearchResult sr = search.search(new Query().setWithScores().setWithPayload());
        assertEquals(1, sr.totalResults);

        Document doc = sr.docs.get(0);
        assertEquals(Double.POSITIVE_INFINITY, doc.getScore(), 0);
        assertNull(doc.getPayload());
        assertEquals(json.toString(), doc.get(JSON_ROOT));

        // query repeat
        sr = search.search(new Query().setWithScores().setWithPayload());

        doc = sr.docs.get(0);
        JSONObject jsonRead = new JSONObject((String) doc.get(JSON_ROOT));
        assertEquals(json.toString(), jsonRead.toString());

        // query repeat
        sr = search.search(new Query().setWithScores().setWithPayload());

        doc = sr.docs.get(0);
        jsonRead = new JSONObject(doc.getString(JSON_ROOT));
        assertEquals(json.toString(), jsonRead.toString());
    }

    @Test
    public void parseJsonPartial() {
        Schema schema = new Schema();
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON);

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        String id = "student:1111";
        JSONObject json = toJson("first", "Joe", "last", "Dod", "age", 18);
        try (Jedis jedis = search.connection()) {
            setJson(jedis, id, json);
        }

        // query
        SearchResult sr = search.search(new Query().returnFields("$.first", "$.last", "$.age"));
        assertEquals(1, sr.totalResults);

        Document doc = sr.docs.get(0);
        assertEquals("Joe", doc.get("$.first"));
        assertEquals("Dod", doc.get("$.last"));
        assertEquals(Integer.toString(18), doc.get("$.age"));

        // query repeat
        sr = search.search(new Query().returnFields("$.first", "$.last", "$.age"));

        doc = sr.docs.get(0);
        assertEquals("Joe", doc.getString("$.first"));
        assertEquals("Dod", doc.getString("$.last"));
        assertEquals(18, Integer.parseInt((String) doc.get("$.age")));
    }

    @Test
    public void parseJsonPartialWithFieldNames() {
        Schema schema = new Schema();
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON);

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        String id = "student:1111";
        JSONObject json = toJson("first", "Joe", "last", "Dod", "age", 18);
        try (Jedis jedis = search.connection()) {
            setJson(jedis, id, json);
        }

        // query
        SearchResult sr = search.search(new Query().returnFields(FieldName.of("$.first").as("first"),
                FieldName.of("$.last").as("last"), FieldName.of("$.age")));
        assertEquals(1, sr.totalResults);

        Document doc = sr.docs.get(0);
        assertNull(doc.get("$.first"));
        assertNull(doc.get("$.last"));
        assertEquals(Integer.toString(18), doc.get("$.age"));
        assertEquals("Joe", doc.get("first"));
        assertEquals("Dod", doc.get("last"));
        assertNull(doc.get("age"));
    }
}
