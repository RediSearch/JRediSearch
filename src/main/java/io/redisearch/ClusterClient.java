package io.redisearch;

import java.util.List;

public interface ClusterClient extends Client {

    List<Object> broadcast(String... args);
}
