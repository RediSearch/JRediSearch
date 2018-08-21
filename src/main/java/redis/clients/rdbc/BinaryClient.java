package redis.clients.rdbc;

import redis.clients.jedis.commands.ProtocolCommand;

import java.util.List;

/**
 * The Binary Client that provides a contract between the lower level implementers of the protocol to the higher level
 * java api developers for specific implementations like for plugins in-case of search and more...
 */
public interface BinaryClient {

    void sendCommand(final ProtocolCommand cmd, final byte[]... args);

    void sendCommand(final ProtocolCommand cmd, final String... args);

    String getStatusCodeReply();

    List<Object> getObjectMultiBulkReply();

    Long getIntegerReply();

    List<String> getMultiBulkReply();

}
