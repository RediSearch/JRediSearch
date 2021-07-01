package io.redisearch;

import java.util.Collection;

import redis.clients.jedis.util.SafeEncoder;

public class FieldName {

    private static final String AS_ENCODED = "AS";
    private static final byte[] AS_BINARY = SafeEncoder.encode(AS_ENCODED);

    private final String name;
    private String alias;

    public FieldName(String name) {
        this(name, null);
    }

    public FieldName(String name, String alias) {
        this.name = name;
        this.alias = alias;
    }

    public int addCommandEncodedArguments(Collection<String> args) {
        args.add(name);
        if (alias == null) {
            return 1;
        }

        args.add(AS_ENCODED);
        args.add(alias);
        return 3;
    }

    public int addCommandBinaryArguments(Collection<byte[]> args) {
        args.add(SafeEncoder.encode(name));
        if (alias == null) {
            return 1;
        }

        args.add(AS_BINARY);
        args.add(SafeEncoder.encode(alias));
        return 3;
    }

    String getName() {
        return name;
    }

    @Override
    public String toString() {
        return alias == null ? name : (name + " AS " + alias);
    }

    public static FieldName of(String name) {
        return new FieldName(name);
    }

    public FieldName as(String alias) {
        this.alias = alias;
        return this;
    }

    public static FieldName[] convert(String... names) {
        if (names == null) return null;
        FieldName[] fields = new FieldName[names.length];
        for (int i = 0; i < names.length; i++)
            fields[i] = FieldName.of(names[i]);
        return fields;
    }
}
