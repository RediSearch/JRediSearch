package io.redisearch.client.util;

import java.util.HashMap;
import java.util.Map;

public class ClientUtil {

  public static Map<String, String> toStringMap(Map<String, Object> input) {
    Map<String, String> output = new HashMap<>(input.size());
    for (Map.Entry<String, Object> entry : input.entrySet()) {
      String key = entry.getKey();
      Object obj = entry.getValue();
      String str;
      if (obj instanceof redis.clients.jedis.GeoCoordinate) {
        redis.clients.jedis.GeoCoordinate geo = (redis.clients.jedis.GeoCoordinate) obj;
        str = geo.getLongitude() + "," + geo.getLatitude();
      } else if (obj instanceof String) {
        str = (String) obj;
      } else {
        str = obj.toString();
      }
      output.put(key, str);
    }
    return output;
  }

  private ClientUtil() {
    throw new InstantiationError("Must not instantiate this class");
  }
}
