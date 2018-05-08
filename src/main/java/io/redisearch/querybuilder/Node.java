package io.redisearch.querybuilder;

import io.redisearch.Query;

/**
 * Created by mnunberg on 2/23/18.
 *
 * Base node interface
 */
public interface Node {
     enum ParenMode {
         /** Always encapsulate */
         ALWAYS,

         /**
          * Never encapsulate. Note that this may be ignored if parentheses are semantically required (e.g.
          * {@code @foo:(val1|val2)}. However something like {@code @foo:v1 @bar:v2} need not be parenthesized.
          */
         NEVER,

         /**
          * Determine encapsulation based on number of children. If the node only has one child, it is not
          * parenthesized, if it has more than one child, it is parenthesized */
         DEFAULT
    }

    /**
     * Returns the string form of this node.
     * @param mode Whether the string should be encapsulated in parentheses {@code (...)}
     * @return The string query.
     */
    String toString(ParenMode mode);

    /**
     * Returns the string form of this node. This may be passed to {@link io.redisearch.client.Client#search(Query)}
     * @return The query string.
     */
    String toString();
}
