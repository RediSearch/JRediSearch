package io.redisearch.api;

import io.redisearch.Schema;

import java.util.Map;

public interface IndexClient {

    boolean createIndex(Schema schema, io.redisearch.client.Client.IndexOptions options);

    Map<String, Object> getInfo();

    boolean dropIndex(boolean missingOk);
}
