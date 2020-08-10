package io.redisearch;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public enum Keywords implements ProtocolCommand {
  SCHEMA, VERBATIM, NOCONTENT, NOSTOPWORDS, WITHSCORES, WITHPAYLOADS, LANGUAGE, INFIELDS,
  SORTBY, ASC, DESC, PAYLOAD, LIMIT, HIGHLIGHT, FIELDS, TAGS, SUMMARIZE, FRAGS, LEN, SEPARATOR, 
  INKEYS, RETURN, NOSAVE, PARTIAL, REPLACE, FILTER, GEOFILTER, POSITIVE_INFINITY("+inf"), NEGATIVE_INFINITY("-inf"),
  INCR, MAX, FUZZY, DD, DELETE, READ, COUNT, ADD, TEMPORARY, STOPWORDS, NOFREQS, NOFIELDS, NOOFFSETS, IF,
  SET, GET, ON, ASYNC, PREFIX, LANGUAGE_FIELD, SCORE_FIELD, SCORE, PAYLOAD_FIELD;
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
