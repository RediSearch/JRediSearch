package io.redisearch.client;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

/**
 * Jedis enum for command encapsulation
 */
 public class Commands {

    public enum Command implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ALTER("FT.ALTER"),
        ADD("FT.ADD"),
        @Deprecated
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        EXPLAIN("FT.EXPLAIN"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        GET("FT.GET"),
        MGET("FT.MGET"),
        AGGREGATE("FT.AGGREGATE"), 
        CURSOR("FT.CURSOR"),
        CONFIG("FT.CONFIG"),
        ALIASADD("FT.ALIASADD"),
        ALIASUPDATE("FT.ALIASUPDATE"),
        ALIASDEL("FT.ALIASDEL"),
        SYNADD("FT.SYNADD"),
        SYNUPDATE("FT.SYNUPDATE"),
        SYNDUMP("FT.SYNDUMP");
      
        private final byte[] raw;

        Command(String alt) {
            raw = SafeEncoder.encode(alt);
        }

        public byte[] getRaw() {
            return raw;
        }
    }
    
    /**
     * @deprecated ClusterCommand is going to be removed in the future
     */
    @Deprecated
    public enum ClusterCommand implements ProtocolCommand {

        CREATE("FT.CREATE"),
        ALTER("FT.ALTER"),
        ADD("FT.ADD"),
        ADDHASH("FT.ADDHASH"),
        INFO("FT.INFO"),
        SEARCH("FT.SEARCH"),
        EXPLAIN("FT.EXPLAIN"),
        DEL("FT.DEL"),
        DROP("FT.DROP"),
        BROADCAST("FT.BROADCAST"),
        GET("FT.GET"),
        MGET("FT.MGET"),
        AGGREGATE("FT.AGGREGATE"),
        CURSOR("FT.CURSOR"),
        CONFIG("FT.CONFIG"),
        ALIASADD("FT.ALIASADD"),
        ALIASUPDATE("FT.ALIASUPDATE"),
        ALIASDEL("FT.ALIASDEL"),
        SYNADD("FT.SYNADD"),
        SYNUPDATE("FT.SYNUPDATE"),
        SYNDUMP("FT.SYNDUMP");
      
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
        ProtocolCommand getAlterCommand();
        ProtocolCommand getAddCommand();
        @Deprecated
        ProtocolCommand getAddHashCommand();
        ProtocolCommand getDelCommand();
        ProtocolCommand getInfoCommand();
        ProtocolCommand getDropCommand();
        ProtocolCommand getSearchCommand();
        ProtocolCommand getExplainCommand();
        ProtocolCommand getGetCommand();
        ProtocolCommand getMGetCommand();
        ProtocolCommand getAggregateCommand();
        ProtocolCommand getCursorCommand();
        ProtocolCommand getConfigCommand();
        ProtocolCommand getAliasAddCommand();
        ProtocolCommand getAliasUpdateCommand();
        ProtocolCommand getAliasDelCommand();
        ProtocolCommand getSynAddCommand();
        ProtocolCommand getSynUpdateCommand();
        ProtocolCommand getSynDumpCommand();
    }

    public static class SingleNodeCommands implements CommandProvider {

        @Override
        public ProtocolCommand getCreateCommand() {
            return Command.CREATE;
        }
        
        @Override
        public ProtocolCommand getAlterCommand() {
            return Command.ALTER;
        }

        @Override
        public ProtocolCommand getAddCommand() {
            return Command.ADD;
        }

        @Deprecated
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

        @Override
        public ProtocolCommand getCursorCommand() {
          return Command.CURSOR;
        }

        @Override
        public ProtocolCommand getConfigCommand() {
            return Command.CONFIG;
        }

        @Override
        public ProtocolCommand getAliasAddCommand() {
            return Command.ALIASADD;
        }

        @Override
        public ProtocolCommand getAliasUpdateCommand() {
            return Command.ALIASUPDATE;
        }

        @Override
        public ProtocolCommand getAliasDelCommand() {
            return Command.ALIASDEL;
        }

        @Override
        public ProtocolCommand getMGetCommand() {
          return Command.MGET;
        }

        @Override
        public ProtocolCommand getSynAddCommand() {
          return Command.SYNADD;
        }

        @Override
        public ProtocolCommand getSynUpdateCommand() {
          return Command.SYNUPDATE;
        }

        @Override
        public ProtocolCommand getSynDumpCommand() {
          return Command.SYNDUMP;
        }
    }

    public static class ClusterCommands implements CommandProvider {

        @Override
        public ProtocolCommand getCreateCommand() {
            return ClusterCommand.CREATE;
        }
        
        @Override
        public ProtocolCommand getAlterCommand() {
            return ClusterCommand.ALTER;
        }

        @Override
        public ProtocolCommand getAddCommand() {
            return ClusterCommand.ADD;
        }

        @Deprecated
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

        @Override
        public ProtocolCommand getCursorCommand() {
          return ClusterCommand.CURSOR;
        }

        @Override
        public ProtocolCommand getConfigCommand() {
            return ClusterCommand.CONFIG;
        }

        @Override
        public ProtocolCommand getAliasAddCommand() {
            return ClusterCommand.ALIASADD;
        }

        @Override
        public ProtocolCommand getAliasUpdateCommand() {
            return ClusterCommand.ALIASUPDATE;
        }

        @Override
        public ProtocolCommand getAliasDelCommand() {
            return ClusterCommand.ALIASDEL;
        }

        @Override
        public ProtocolCommand getMGetCommand() {
          return ClusterCommand.MGET;
        }

        @Override
        public ProtocolCommand getSynAddCommand() {
          return ClusterCommand.SYNADD;
        }

        @Override
        public ProtocolCommand getSynUpdateCommand() {
          return ClusterCommand.SYNUPDATE;
        }

        @Override
        public ProtocolCommand getSynDumpCommand() {
          return ClusterCommand.SYNDUMP;
        }
    }
}
