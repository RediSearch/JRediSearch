package io.redisearch.api;

import java.util.List;

public interface ClusterClient {

    List<Object> broadcast(String... args);
}
