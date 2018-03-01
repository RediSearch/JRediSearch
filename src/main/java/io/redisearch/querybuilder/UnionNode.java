package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 */
public class UnionNode extends QueryNode {
    @Override
    protected String getJoinString() {
        return "|";
    }
}
