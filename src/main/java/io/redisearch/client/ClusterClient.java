package io.redisearch.client;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by dvirvolk on 12/04/2017.
 */
class ClusterClient extends Client implements io.redisearch.ClusterClient {

    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     */
    protected ClusterClient(String indexName, String host, int port, int timeout, int poolSize, String password) {
        super(indexName, host, port, timeout, poolSize, password);
        this.commands = new Commands.ClusterCommands();
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
