package io.redisearch;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Keywords implements ProtocolCommand {
  VERBATIM, NOCONTENT, NOSTOPWORDS, WITHSCORES, WITHPAYLOADS, LANGUAGE, INFIELDS,
  SORTBY, ASC, DESC, PAYLOAD, LIMIT, HIGHLIGHT, FIELDS, TAGS, SUMMARIZE, FRAGS, LEN, SEPARATOR, 
  INKEYS, RETURN, NOSAVE, PARTIAL, REPLACE, FILTER, GEOFILTER, POSITIVE_INFINITY("+inf"), NEGATIVE_INFINITY("-inf");

  private final byte[] raw;
  
  Keywords(String keyword) {
    raw = SafeEncoder.encode(keyword);
  }
  
  Keywords() {
    raw = SafeEncoder.encode(this.name());
  }

  @Override
  public byte[] getRaw() {
    return raw;
  }
}
