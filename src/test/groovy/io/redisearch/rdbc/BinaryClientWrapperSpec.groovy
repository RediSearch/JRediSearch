package io.redisearch.rdbc


import redis.clients.jedis.commands.ProtocolCommand
import spock.lang.Specification

class BinaryClientWrapperSpec extends Specification {

    redis.clients.jedis.BinaryClient binaryClient = Mock(redis.clients.jedis.BinaryClient)
    ProtocolCommand protocolCommand = Mock(ProtocolCommand)
    String testValue = "send command arguments"
    byte[] bytes = new byte[10]


    def setup() {
    }

    def "Make sure wrapper is calls underlying wrapped binary client method for string args"() {
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        binaryClientWrapper.sendCommand(protocolCommand, testValue)

        then:
        1 * binaryClient.sendCommand(protocolCommand, testValue)

    }

    def "Make sure wrapper is calls underlying wrapped binary client method for byte[] args"() {
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        binaryClientWrapper.sendCommand(protocolCommand, bytes, bytes)

        then:
        1 * binaryClient.sendCommand(protocolCommand, [bytes, bytes])

    }

    def "Make sure wrapper is calls underlying wrapped getIntegerReply"() {
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        int answer = binaryClientWrapper.getIntegerReply()

        then:
        1 * binaryClient.getIntegerReply() >> 1
        answer == 1

    }

    def "Make sure wrapper is calls underlying wrapped getStatusCodeReply"() {
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        String answer = binaryClientWrapper.getStatusCodeReply()

        then:
        1 * binaryClient.getStatusCodeReply() >> "OK"
        answer == "OK"

    }

    def "Make sure wrapper is calls underlying wrapped getMultiBulkReply"() {
        List<String> values = ["here is a bulk"]
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        List<String> answer = binaryClientWrapper.getMultiBulkReply()

        then:
        1 * binaryClient.getMultiBulkReply() >> values
        answer == values

    }

    def "Make sure wrapper is calls underlying wrapped getObjectMultiBulkReply"() {
        List<Object> values = ["here is a bulk", new Integer("2")]
        when:
        BinaryClientWrapper binaryClientWrapper = new BinaryClientWrapper(binaryClient)
        List<Object> answer = binaryClientWrapper.getObjectMultiBulkReply()

        then:
        1 * binaryClient.getObjectMultiBulkReply() >> values
        answer == values

    }


}
