package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 */
public class DisjunctNode extends IntersectNode {
    @Override
    public String toString(ParenMode mode) {
        String ret = super.toString(ParenMode.NEVER);
        if (shouldUseParens(mode)) {
            return "-("  + ret + ")";
        } else {
            return "-" + ret;
        }
    }
}
