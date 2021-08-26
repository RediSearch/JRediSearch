package io.redisearch.client.util;

import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class UtilTest {

  @Test
  public void geoCoordinate() {
    Map<String, Object> objectMap = Collections.singletonMap("geo", new redis.clients.jedis.GeoCoordinate(22.5, 45.0));
    Map<String, String> stringMap = Collections.singletonMap("geo", (22.5 + "," + 45.0));
    Assert.assertEquals(stringMap, ClientUtil.toStringMap(objectMap));
  }
}
