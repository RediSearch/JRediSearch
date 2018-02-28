package io.redisearch.client;

import redis.clients.jedis.Jedis;

import java.util.List;

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

    public ClusterClient(String indexName, String host, int port) {
        this(indexName, host, port, 500, 100);
    }

    public List<Object> broadcast(String... args) {
        try (Jedis conn = _conn()) {
            List ret = conn.getClient().sendCommand(Commands.ClusterCommand.BROADCAST, args).getObjectMultiBulkReply();
            return ret;
        }
    }
}
