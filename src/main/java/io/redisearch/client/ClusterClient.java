package io.redisearch.client;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.List;
import java.util.Set;

/**
 * Created by dvirvolk on 12/04/2017.
 */
public class ClusterClient extends Client {

    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     */
    public ClusterClient(String indexName, String host, int port, int timeout, int poolSize) {
        super(indexName, host, port, timeout, poolSize);
        this.commands = new Commands.ClusterCommands();
    }

    /**
     * Create a new ClusterClient to a RediSearch index which can connect to password protected
     * Redis Server
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     * @param password  the password for authentication in a password protected Redis server
     */
    public ClusterClient(String indexName, String host, int port, int timeout, int poolSize, String password) {
        super(indexName, host, port, timeout, poolSize, password);
        this.commands = new Commands.ClusterCommands();
    }

    public ClusterClient(String indexName, String host, int port) {
        this(indexName, host, port, 500, 100);
    }

    /**
     * Creates a new ClusterClient to a RediSearch index with JedisSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     * 
     * @param indexName the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels the set of sentinels monitoring the cluster
     * @param timeout the timeout in milliseconds
     * @param poolSize the poolSize of JedisSentinelPool
     * @param password the password for authentication in a password protected Redis server
     */
    public ClusterClient(String indexName, String masterName, Set<String> sentinels, int timeout, int poolSize, String password) {
        super(indexName, masterName, sentinels, timeout, poolSize, password);
        this.commands = new Commands.ClusterCommands();
    }

    /**
     * Creates a new ClusterClient to a RediSearch index with JedisSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     * 
     * <p>The Client is initialized with following default values for {@link JedisSentinelPool}
     * <ul><li> password - NULL, no authentication required to connect to Redis Server</li></ul>
     * 
     * @param indexName the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels the set of sentinels monitoring the cluster
     * @param timeout the timeout in milliseconds
     * @param poolSize the poolSize of JedisSentinelPool
     */
    public ClusterClient(String indexName, String masterName, Set<String> sentinels, int timeout, int poolSize) {
        this(indexName, masterName, sentinels, timeout, poolSize, null);
    }

    /**
     * Creates a new ClusterClient to a RediSearch index with JedisSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     * 
     * <p>The Client is initialized with following default values for {@link JedisSentinelPool}
     * <ul> <li>timeout - 500 mills</li>
     * <li> poolSize - 100 connections</li>
     * <li> password - NULL, no authentication required to connect to Redis Server</li></ul>
     * 
     * @param indexName the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels the set of sentinels monitoring the cluster
     */
    public ClusterClient(String indexName, String masterName, Set<String> sentinels) {
        this(indexName, masterName, sentinels, 500, 100);
    }

    public List<Object> broadcast(String... args) {
        try (Jedis conn = _conn()) {
            BinaryClient client = conn.getClient();
            client.sendCommand(Commands.ClusterCommand.BROADCAST, args);
            List ret = client.getObjectMultiBulkReply();
            return ret;
        }
    }
}
