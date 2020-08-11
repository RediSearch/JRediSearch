package io.redisearch.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum ConfigOption implements ProtocolCommand {
    NOGC,
    MINPREFIX,
    MAXEXPANSIONS,
    TIMEOUT,
    ON_TIMEOUT,
    MIN_PHONETIC_TERM_LEN,
    ALL("*");
  
    private final byte[] raw;

    ConfigOption() {
      raw = SafeEncoder.encode(this.name());
    }
    
    ConfigOption(String name) {
        raw = SafeEncoder.encode(name);
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
}
