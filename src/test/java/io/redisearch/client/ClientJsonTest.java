package io.redisearch.client;

import io.redisearch.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.ProtocolCommand;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClientJsonTest {

    private enum JsonCommands implements ProtocolCommand {

        SET;

        @Override
        public byte[] getRaw() {
            return ("JSON." + name()).getBytes();
        }
    }

    private static final String INDEX_NAME = "json-index";

    private static Client search;

    @Before
    public void setUp() {
        search = new Client(INDEX_NAME, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
        try (Jedis j = search.connection()) {
            j.flushAll();
        }
    }

    @After
    public void tearDown() {
        search.close();
    }

    private static void setJson(Jedis jedis, String key, JSONObject json) {
        jedis.sendCommand(JsonCommands.SET, key, ".", json.toString());
    }

    private static JSONObject toJson(Object... values) {
        JSONObject json = new JSONObject();
        for (int i = 0; i < values.length; i += 2) {
            json.put((String) values[i], values[i + 1]);
        }
        return json;
    }

    @Test
    public void create() throws Exception {
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
    public void parse() throws Exception {
        Schema schema = new Schema().addTextField("first", 1.0).addTextField("last", 1.0)
                .addNumericField("age");
        IndexDefinition rule = new IndexDefinition(IndexDefinition.Type.JSON)
                .setPrefixes(new String[]{"student:", "pupil:"});

        assertTrue(search.createIndex(schema, Client.IndexOptions.defaultOptions().setDefinition(rule)));

        String id = "student:1111";
        JSONObject json = toJson("first", "Joe", "last", "Dod", "age", 18);
        try (Jedis jedis = search.connection()) {
            setJson(jedis, id, json);
        }

        SearchResult sr = search.search(new Query().setWithScores().setWithPayload());
        assertEquals(1, sr.totalResults);

        Document doc = sr.docs.get(0);
        assertEquals(Double.POSITIVE_INFINITY, doc.getScore(), 0);
        assertNull(doc.getPayload());
        assertEquals(json.toString(), doc.getJsonProperties().toString());
    }
}
