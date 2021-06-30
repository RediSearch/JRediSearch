package io.redisearch.client;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.junit.Before;

public abstract class TestBase {

    static String TEST_INDEX;

    static Client search;

    static Client createClient(String indexName) {
        return new Client(indexName, Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
    }

    protected Client getDefaultClient() {
        return search;
    }

    static void prepare() {
        search = createClient(TEST_INDEX);
    }

    @Before
    public void setUp() {
        try (Jedis j = search.connection()) {
            j.flushAll();
        }
    }

    static void tearDown() {
        search.close();
    }
}
