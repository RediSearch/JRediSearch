package io.redisearch;

import java.util.List;

/**
 * @deprecated ClusterClient is going to be removed in the future
 */
@Deprecated
public interface ClusterClient extends Client {

    List<Object> broadcast(String... args);
}
