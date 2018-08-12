package io.redisearch;

import java.util.List;

public interface ClusterClient extends SearchClient {

    List<Object> broadcast(String... args);
}
