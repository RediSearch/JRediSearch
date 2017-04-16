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

    public enum ClusterCommand implements ProtocolCommand {

        CREATE("DFT.CREATE"),
        ADD("DFT.ADD"),
        ADDHASH("DFT.ADDHASH"),
        INFO("DFT.INFO"),
        SEARCH("DFT.FSEARCH"),
        DEL("DFT.DEL"),
        DROP("DFT.DROP"),
        OPTIMIZE("DFT.OPTIMIZE"),
        BROADCAST("DFT.BROADCAST");
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
        ProtocolCommand getOptimizeCommand();
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
        public ProtocolCommand getOptimizeCommand() {
            return Command.OPTIMIZE;
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
        public ProtocolCommand getOptimizeCommand() {
            return ClusterCommand.OPTIMIZE;
        }
    }

}
