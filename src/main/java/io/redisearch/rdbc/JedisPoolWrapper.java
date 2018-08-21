package io.redisearch.rdbc;

import redis.clients.jedis.Jedis;
import redis.clients.rdbc.Connection;
import redis.clients.rdbc.Pool;

/**
 * This class hopefully will go away after the API definition is adopted for the rdbc api
 */
public class JedisPoolWrapper implements Pool<Connection> {

    private redis.clients.jedis.util.Pool<Jedis> pool;

    public JedisPoolWrapper(redis.clients.jedis.util.Pool<Jedis> pool) {
        this.pool = pool;
    }

    @Override
    public Connection getResource() {
        return new JedisWrapper(pool.getResource());
    }

    @Override
    public void close(){
        this.pool.close();
    }
}
