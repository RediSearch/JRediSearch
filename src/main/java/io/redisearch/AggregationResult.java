package io.redisearch;

import io.redisearch.aggregation.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mnunberg on 2/22/18.
 */
public class AggregationResult {
	
    private final List<Map<String, Object>> results = new ArrayList<>();
    
    public AggregationResult(List<Object> resp) {
        for (int i = 1; i < resp.size(); i++) {
            Object o = resp.get(i);
            List<Object> raw = (List<Object>)o;
            Map<String, Object> cur = new HashMap<>();
            for (int j = 0; j < raw.size(); j += 2) {
                cur.put(new String((byte[])raw.get(j)), raw.get(j+1));
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
}
