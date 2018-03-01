package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 */
public class DisjunctUnionNode extends DisjunctNode {
    @Override
    protected String getJoinString() {
        return "|";
    }
}
