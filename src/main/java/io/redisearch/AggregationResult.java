package io.redisearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mnunberg on 2/22/18.
 */
public class AggregationResult {
    private final List<Map<String, String>> results = new ArrayList<>();
    public AggregationResult(List<Object> resp) {
        for (Object o : resp) {
            List<Object> raw = (List<Object>)o;
            Map<String, String> cur = new HashMap<>();
            for (int i = 0; i < raw.size(); i += 2) {
                cur.put(raw.get(i).toString(), raw.get(i+1).toString());
            }
            results.add(cur);
        }
    }

    public List<Map<String, String>> getResults() {
        return results;
    }
}
