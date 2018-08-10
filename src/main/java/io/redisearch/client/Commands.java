package io.redisearch.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

/**
 * Jedis enum for command encapsulation
 */
 public class Commands {


    public enum GeneralKey implements  ProtocolCommand {

        INCREMENT("INCR"),
        PAYLOAD("PAYLOAD"),
        WITHPAYLOADS("WITHPAYLOADS"),
        MAX("MAX"),
        FUZZY("FUZZY"),
        SCORES("WITHSCORES");

        private final byte[] raw;

        GeneralKey(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    // TODO: Move this to the client and autocompleter as two different enums
    public enum Command implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ADD("FT.ADD"),
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        EXPLAIN("FT.EXPLAIN"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        GET("FT.GET"),
        AGGREGATE("FT.AGGREGATE");
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    public enum ClusterCommand implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ADD("FT.ADD"),
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        EXPLAIN("FT.EXPLAIN"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        BROADCAST("FT.BROADCAST"),
        GET("FT.GET"),
        AGGREGATE("FT.AGGREGATE");
        private final byte[] raw;

        ClusterCommand(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }

    public interface CommandProvider {
        ProtocolCommand getCreateCommand();
        ProtocolCommand getAddCommand();
        ProtocolCommand getAddHashCommand();
        ProtocolCommand getDelCommand();
        ProtocolCommand getInfoCommand();
        ProtocolCommand getDropCommand();
        ProtocolCommand getSearchCommand();
        ProtocolCommand getExplainCommand();
        ProtocolCommand getGetCommand();
        ProtocolCommand getAggregateCommand();
    }

    public static class SingleNodeCommands implements CommandProvider {

        @Override
        public ProtocolCommand getCreateCommand() {
            return Command.CREATE;
        }

        @Override
        public ProtocolCommand getAddCommand() {
            return Command.ADD;
        }

        @Override
        public ProtocolCommand getAddHashCommand() {
            return Command.ADDHASH;
        }

        @Override
        public ProtocolCommand getDelCommand() {
            return Command.DEL;
        }

        @Override
        public ProtocolCommand getInfoCommand() {
            return Command.INFO;
        }

        @Override
        public ProtocolCommand getDropCommand() {
            return Command.DROP;
        }

        @Override
        public ProtocolCommand getSearchCommand() {
            return Command.SEARCH;
        }

        @Override
        public ProtocolCommand getExplainCommand() {
            return Command.EXPLAIN;
        }

        @Override
        public ProtocolCommand getGetCommand() {
            return Command.GET;
        }

        @Override
        public ProtocolCommand getAggregateCommand() {
            return Command.AGGREGATE;
        }
    }

    public static class ClusterCommands implements CommandProvider {

        @Override
        public ProtocolCommand getCreateCommand() {
            return ClusterCommand.CREATE;
        }

        @Override
        public ProtocolCommand getAddCommand() {
            return ClusterCommand.ADD;
        }

        @Override
        public ProtocolCommand getAddHashCommand() {
            return ClusterCommand.ADDHASH;
        }

        @Override
        public ProtocolCommand getDelCommand() {
            return ClusterCommand.DEL;
        }

        @Override
        public ProtocolCommand getInfoCommand() {
            return ClusterCommand.INFO;
        }

        @Override
        public ProtocolCommand getDropCommand() {
            return ClusterCommand.DROP;
        }

        @Override
        public ProtocolCommand getSearchCommand() {
            return ClusterCommand.SEARCH;
        }

        @Override
        public ProtocolCommand getExplainCommand() {
            return ClusterCommand.EXPLAIN;
        }

        @Override
        public ProtocolCommand getGetCommand() {
            return ClusterCommand.GET;
        }

        @Override
        public ProtocolCommand getAggregateCommand() {
            return ClusterCommand.AGGREGATE;
        }
    }
}
