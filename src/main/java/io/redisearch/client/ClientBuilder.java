package io.redisearch.client;

import io.redisearch.SearchClient;

public class ClientBuilder {
    private String indexName;
    private String host;
    private int port = 6379;
    private int timeout = 500;
    private int poolSize = 100;
    private String password = null;

    /**
     * @param indexName the name of the index we are connecting to or creating
     * @return builder
     */
    public ClientBuilder indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    /**
     * @param host redis host
     * @return builder
     */
    public ClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * If not provided than the redis default 6379 is used
     *
     * @param port redis port
     * @return
     */
    public ClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    public ClientBuilder timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public ClientBuilder poolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public ClientBuilder password(String password) {
        this.password = password;
        return this;
    }


    /**
     * Build a new client to a RediSearch index
     *
     * @return the interface to the Search for Redis
     */
    public SearchClient build() {
        validate();
        return new Client(indexName, host, port, timeout, poolSize, password);
    }

    /**
     * Build a new clent to a RediSearch index with added benefit for cluster
     * @return the interface to the Search for Redis with cluster
     */
    public ClusterClient buildCluster() {
        validate();
        return new ClusterClient(indexName, host, port, timeout, poolSize, password);
    }


    protected void validate() {
        String missing = "";
        if (this.indexName == null) {
            missing += " indexName";
        }
        if (this.host == null) {
            missing += " host";
        }

        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing required fields:" + missing);
        }

    }


    public static ClientBuilder builder() {
        return new ClientBuilder();
    }
}