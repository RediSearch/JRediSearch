package io.redisearch.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.util.SafeEncoder;

/**
 * Created by dvirsky on 08/02/17.
 */
public class Commands {
    public enum Command implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ADD("FT.ADD"),
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        OPTIMIZE("FT.OPTIMIZE"),
        SUGADD("FT.SUGADD"),
        SUGGET("FT.SUGGET"),
        SUGDEL("FT.SUGDEL"),
        SUGLEN("FT.SUGLEN");


        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

}
