package io.redisearch.client;

public enum ConfigOption {
    NOGC("NOGC"),
    MINPREFIX("MINPREFIX"),
    MAXEXPANSIONS("MAXEXPANSIONS"),
    TIMEOUT("TIMEOUT"),
    ON_TIMEOUT("ON_TIMEOUT"),
    MIN_PHONETIC_TERM_LEN("MIN_PHONETIC_TERM_LEN"),
    ALL("*");

    private final String name;

    ConfigOption(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
