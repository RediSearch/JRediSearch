package io.redisearch.rdbc;

import redis.clients.jedis.Jedis;
import redis.clients.rdbc.BinaryClient;
import redis.clients.rdbc.Connection;

import java.util.Map;
import java.util.Set;

public class JedisWrapper implements Connection {

    private Jedis jedis;

    public JedisWrapper(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public BinaryClient getClient() {
        return new BinaryClientWrapper(this.jedis.getClient());
    }

    @Override
    public void close() {
        this.jedis.close();
    }

    @Override
    public void flushDB() {
        this.jedis.flushDB();
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return this.jedis.hmset(key, hash);
    }

    @Override
    public Set<String> keys(String pattern) {
        return this.jedis.keys(pattern);
    }

    @Override
    public String ping(String message) {
        return this.jedis.ping(message);
    }


}
