package io.redisearch.rdbc;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.rdbc.BinaryClient;

import java.util.List;

/**
 * Used to wrap Jedis just until it starts to adopt the generic Interface contract
 */
public class BinaryClientWrapper implements BinaryClient {

    private redis.clients.jedis.BinaryClient binaryClient;

    public BinaryClientWrapper(redis.clients.jedis.BinaryClient binaryClient) {
        this.binaryClient = binaryClient;
    }

    @Override
    public void sendCommand(ProtocolCommand cmd, byte[]... args) {
        this.binaryClient.sendCommand(cmd, args);
    }

    @Override
    public void sendCommand(ProtocolCommand cmd, String... args) {
        this.binaryClient.sendCommand(cmd, args);
    }

    @Override
    public String getStatusCodeReply() {
        return this.binaryClient.getStatusCodeReply();
    }

    @Override
    public List<Object> getObjectMultiBulkReply() {
        return this.binaryClient.getObjectMultiBulkReply();
    }

    @Override
    public Long getIntegerReply() {
        return this.binaryClient.getIntegerReply();
    }

    @Override
    public List<String> getMultiBulkReply() {
        return this.binaryClient.getMultiBulkReply();
    }

}
