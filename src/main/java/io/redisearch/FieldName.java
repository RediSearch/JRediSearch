package io.redisearch;

import java.util.Collection;

import redis.clients.jedis.util.SafeEncoder;

public class FieldName {

    private static final String AS_ENCODED = "AS";
    private static final byte[] AS_BINARY = SafeEncoder.encode(AS_ENCODED);

    private final String name;
    private final String fieldAlias;

    public FieldName(String name) {
        this(name, null);
    }

    public FieldName(String name, String alias) {
        this.name = name;
        this.fieldAlias = alias;
    }

    public void addCommandEncodedArguments(Collection<String> args) {
        args.add(name);
        if (fieldAlias != null) {
            args.add(AS_ENCODED);
            args.add(fieldAlias);
        }
    }

    public int addCommandBinaryArguments(Collection<byte[]> args) {
        args.add(SafeEncoder.encode(name));
        if (fieldAlias == null) {
            return 1;
        }

        args.add(AS_BINARY);
        args.add(SafeEncoder.encode(fieldAlias));
        return 3;
    }

    public String getName() {
        return name;
    }

    public String getFieldAlias() {
        return fieldAlias;
    }

    @Override
    public String toString() {
        return fieldAlias == null ? name : (name + " AS " + fieldAlias);
    }

    public static FieldName of(String name) {
        return new FieldName(name);
    }

    public static FieldName of(String name, String alias) {
        return new FieldName(name, alias);
    }
}
