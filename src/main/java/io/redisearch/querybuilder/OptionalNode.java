package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 *
 * The optional node affects scoring and ordering. If it evaluates to true, the result is ranked
 * higher. It is helpful to combine it with a {@link UnionNode} to rank a document higher if it meets
 * one of several criteria.
 *
 * In RS: {@code ~(@lang:en @country:us)}.
 */
public class OptionalNode extends IntersectNode {
    @Override
    public String toString(ParenMode mode) {
        String ret = super.toString(ParenMode.NEVER);
        if (shouldUseParens(mode)) {
            return "~(" + ret + ")";
        }
        return "~" + ret;
    }
}
