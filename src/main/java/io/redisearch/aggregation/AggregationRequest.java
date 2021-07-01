package io.redisearch.aggregation;

import io.redisearch.FieldName;
import io.redisearch.aggregation.reducers.Reducer;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

/**
 * Created by mnunberg on 2/22/18.
 * @deprecated use {@link io.redisearch.aggregation.AggregationBuilder} instead
 */
@Deprecated
public class AggregationRequest {
    private String query;
    private final List<FieldName> load = new ArrayList<>();
    private final List<Group> groups = new ArrayList<>();
    private final List<SortedField> sortby = new ArrayList<>();
    private final Map<String, String> projections = new HashMap<>();
    private final Map<String, String> queryProjections = new HashMap<>();
    private String filterQuery;

    private Limit limit = Limit.NO_LIMIT;
    private int sortbyMax = 0;
    
    private int cursorCount = 0;
    private long cursorMaxIdle = Long.MAX_VALUE;

    public AggregationRequest(String query) {
        this.query = query;
    }

    public AggregationRequest() {
        this("*");
    }

    public AggregationRequest load(String... fields) {
        return load(FieldName.convert(fields));
    }

    public AggregationRequest load(FieldName... fields) {
        load.addAll(Arrays.asList(fields));
        return this;
    }

    public AggregationRequest limit(int offset, int count) {
        if (groups.isEmpty()) {
            limit = new Limit(offset, count);
        } else {
            groups.get(groups.size()-1).limit(new Limit(offset, count));
        }
        return this;
    }

    public AggregationRequest limit(int count) {
        return limit(0, count);
    }

    public AggregationRequest sortBy(SortedField ...fields) {
        sortby.addAll(Arrays.asList(fields));
        return this;
    }

    public AggregationRequest sortBy(Collection<SortedField> fields, int max) {
        sortby.addAll(fields);
        sortbyMax = max;
        return this;
    }

    public AggregationRequest sortBy(SortedField field, int max) {
        sortBy(field);
        sortbyMax = max;
        return this;
    }

    public AggregationRequest sortByAsc(String field) {
        return sortBy(SortedField.asc(field));
    }

    public AggregationRequest sortByDesc(String field) {
        return sortBy(SortedField.desc(field));
    }
    
    /**
     * @deprecated use {@link #groupApply(String, String)} instead
     */
    @Deprecated
    public AggregationRequest apply(String projection, String alias) {
      groupApply(projection, alias);
      return this;
    }

    public AggregationRequest groupApply(String projection, String alias) {
        projections.put(alias, projection);
        return this;
    }
    
    public AggregationRequest queryApply(String projection, String alias) {
    	queryProjections.put(alias, projection);
        return this;
    }

    public AggregationRequest groupBy(Collection<String> fields, Collection<Reducer> reducers) {
        String[] fieldsArr = new String[fields.size()];
        Group g = new Group(fields.toArray(fieldsArr));
        for (Reducer r : reducers) {
            g.reduce(r);
        }
        groups.add(g);
        return this;
    }

    public AggregationRequest groupBy(String field, Reducer ...reducers) {
        return groupBy(Collections.singletonList(field), Arrays.asList(reducers));
    }

    public AggregationRequest groupBy(Group group) {
        groups.add(group);
        return this;
    }
    
    public AggregationRequest filter(String expression) {
   		filterQuery=expression;
    	return this;
    }
    
    public AggregationRequest cursor(int count, long maxIdle) {
      this.cursorCount = count;
      this.cursorMaxIdle = maxIdle;
      return this;
    }

    public List<String> getArgs() {
        List<String> args = new ArrayList<>();
        args.add(query);

        if (!load.isEmpty()) {
            args.add("LOAD");
            final int loadCountIndex = args.size();
            args.add(null);
            int loadCount = 0;
            for (FieldName fn : load) {
                loadCount += fn.addCommandEncodedArguments(args);
            }
            args.set(loadCountIndex, Integer.toString(loadCount));
        }

        if (!queryProjections.isEmpty()) {
            for (Map.Entry<String, String> e : queryProjections.entrySet()) {
            	args.add("APPLY");
                args.add(e.getValue());
                args.add("AS");
                args.add(e.getKey());
            }
        }
        
        if (!groups.isEmpty()) {
            for (Group group : groups) {
                args.add("GROUPBY");
                group.addArgs(args);
            }
        }

        if (!projections.isEmpty()) {
            for (Map.Entry<String, String> e : projections.entrySet()) {
            	args.add("APPLY");
                args.add(e.getValue());
                args.add("AS");
                args.add(e.getKey());
            }
        }

        if(filterQuery!=null) {
        	args.add("FILTER");
        	args.add(filterQuery);
        }
        
        if (!sortby.isEmpty()) {
            args.add("SORTBY");
            args.add(Integer.toString(sortby.size() * 2));
            for (SortedField field : sortby) {
                args.add(field.getField());
                args.add(field.getOrder());
            }
            if (sortbyMax > 0) {
                args.add("MAX");
                args.add(Integer.toString(sortbyMax));
            }
        }

        limit.addArgs(args);
        
        if(cursorCount > 0) {
          args.add("WITHCURSOR");
          args.add("COUNT");
          args.add(Integer.toString(cursorCount));
          if(cursorMaxIdle < Long.MAX_VALUE && cursorMaxIdle>=0) {
            args.add("MAXIDLE");
            args.add(Long.toString(cursorMaxIdle));
          }
        }
        
        return args;
    }

    public void serializeRedisArgs(List<byte[]> redisArgs) {
        for (String s : getArgs()) {
            redisArgs.add(SafeEncoder.encode(s));
        }
    }

    public String getArgsString() {
        StringJoiner sj = new StringJoiner(" ");
        for (String s : getArgs()) {
            sj.add(s);
        }
        return sj.toString();
    }
    
    public boolean isWithCursor() {
        return cursorCount > 0;
    }
}
