package io.redisearch;

import io.redisearch.aggregation.Row;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mnunberg on 2/22/18.
 */
public class AggregationResult {

    public final long totalResults;

    private long cursorId = -1;
    private final List<Map<String, Object>> results = new ArrayList<>();
    
    public AggregationResult(List<Object> resp, long cursorId) {
      this(resp);
      this.cursorId = cursorId;
    }
    public AggregationResult(List<Object> resp) {
        // the first element is always the number of results
        totalResults = (Long) resp.get(0);    	

        for (int i = 1; i < resp.size(); i++) {
            List<Object> raw = (List<Object>)resp.get(i);
            Map<String, Object> cur = new HashMap<>();
            for (int j = 0; j < raw.size(); j += 2) {
                Object r = raw.get(j);
                if(r instanceof JedisDataException) {
                  throw (JedisDataException)r;
                }
                cur.put(new String((byte[])r), raw.get(j+1));
            }
            results.add(cur);
        }
    }

    public List<Map<String, Object>> getResults() {
        return results;
    }
    public Row getRow(int index) {
        if (index >= results.size()) {
            return null;
        }
        return new Row(results.get(index));
    }
    
    public long getCursorId() {
      return cursorId;
    }
}
