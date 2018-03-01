package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 */
public interface Node {
     enum ParenMode {
        ALWAYS, NEVER, DEFAULT
    }

    String toString(ParenMode mode);
}
