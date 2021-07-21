package io.redisearch;

import java.util.Collection;

import redis.clients.jedis.util.SafeEncoder;

public class FieldName {

    private static final String AS_ENCODED = "AS";
    private static final byte[] AS_BINARY = SafeEncoder.encode(AS_ENCODED);

    private final String name;
    private String attribute;

    public FieldName(String name) {
        this(name, null);
    }

    public FieldName(String name, String attribute) {
        this.name = name;
        this.attribute = attribute;
    }

    public int addCommandEncodedArguments(Collection<String> args) {
        args.add(name);
        if (attribute == null) {
            return 1;
        }

        args.add(AS_ENCODED);
        args.add(attribute);
        return 3;
    }

    public int addCommandBinaryArguments(Collection<byte[]> args) {
        args.add(SafeEncoder.encode(name));
        if (attribute == null) {
            return 1;
        }

        args.add(AS_BINARY);
        args.add(SafeEncoder.encode(attribute));
        return 3;
    }

    String getName() {
        return name;
    }

    @Override
    public String toString() {
        return attribute == null ? name : (name + " AS " + attribute);
    }

    public static FieldName of(String name) {
        return new FieldName(name);
    }

    public FieldName as(String attribute) {
        this.attribute = attribute;
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
