package io.redisearch.querybuilder;

/**
 * Created by mnunberg on 2/23/18.
 */
public class QueryBuilder {
    private QueryBuilder() {
    }

    public static QueryNode intersect(Node ...n) {
        return new IntersectNode().add(n);
    }

    public static QueryNode intersect(String field, Value value) {
        return intersect().add(field, value);
    }

    public static QueryNode union(Node ...n) {
        return new UnionNode().add(n);
    }

    public static QueryNode union(String field, Value ...values) {
        return union().add(field, values);
    }


    public static QueryNode disjunct(Node ...n) {
        return new DisjunctNode().add(n);
    }

    public static QueryNode disjunct(String field, Value ...values) {
        return disjunct().add(field, values);
    }

    public static QueryNode disjunctUnion(Node ...n) {
        return new DisjunctUnionNode().add(n);
    }

    public static QueryNode disjunctUnion(String field, Value ...values) {
        return disjunctUnion().add(field, values);
    }

    public static QueryNode optional(Node ...n) {
        return new OptionalNode().add(n);
    }

    public static QueryNode optional(String field, Value ...values) {
        return optional().add(field, values);
    }
}