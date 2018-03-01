package io.redisearch.querybuilder;

import java.util.*;

public abstract class QueryNode implements Node {
    private List<Node> children = new ArrayList<>();

    protected abstract String getJoinString();

    public QueryNode add(String field, Value ...values) {
        children.add(new ValueNode(field, getJoinString(), values));
        return this;
    }

    public QueryNode add(String field, String ...values) {
        children.add(new ValueNode(field, getJoinString(), values));
        return this;
    }

    public QueryNode add(String field, Collection<Value> values) {
        return add(field, (Value[])values.toArray());
    }

    public QueryNode add(Node ...nodes) {
        children.addAll(Arrays.asList(nodes));
        return this;
    }

    protected boolean shouldUseParens(ParenMode mode) {
        if (mode == ParenMode.ALWAYS) {
            return true;
        } else if (mode == ParenMode.NEVER) {
            return false;
        } else {
            return children.size() > 1;
        }
    }

    @Override
    public String toString(ParenMode parenMode) {
        StringBuilder sb = new StringBuilder();
        StringJoiner sj = new StringJoiner(getJoinString());
        if (shouldUseParens(parenMode)) {
            sb.append("(");
        }
        for (Node n : children) {
            sj.add(n.toString(parenMode));
        }
        sb.append(sj.toString());
        if (shouldUseParens(parenMode)) {
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(ParenMode.DEFAULT);
    }
}