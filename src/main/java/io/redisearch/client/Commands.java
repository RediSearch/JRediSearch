package io.redisearch.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.util.SafeEncoder;

/**
 * Jedis enum for command encapsulation
 */
public class Commands {
    // TODO: Move this to the client and autocompleter as two different enums
    public enum Command implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ADD("FT.ADD"),
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        OPTIMIZE("FT.OPTIMIZE");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

}
