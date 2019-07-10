package io.redisearch.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mnunberg on 2/22/18.
 */
public class Limit {
    public static final Limit NO_LIMIT = new Limit(0,0);
  
    private final int offset;
    private final int count;

    public Limit(int offset, int count) {
        this.offset = offset;
        this.count = count;
    }

    public List<String> getArgs() {
        if (count == 0){
            return Collections.emptyList();
        }
        List<String> ll = new ArrayList<>(3);
        ll.add("LIMIT");
        ll.add(Integer.toString(offset));
        ll.add(Integer.toString(count));
        return ll;
    }
}
