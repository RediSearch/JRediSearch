package io.redisearch.querybuilder;

import io.redisearch.FieldName;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.aggregation.Group;
import java.util.Arrays;
import org.junit.Test;

import static io.redisearch.aggregation.SortedField.desc;
import static io.redisearch.aggregation.reducers.Reducers.*;
import static io.redisearch.querybuilder.QueryBuilder.*;
import static io.redisearch.querybuilder.Values.*;
import static org.junit.Assert.*;


/**
 * Created by mnunberg on 2/23/18.
 */
public class BuilderTest {
    @Test
    public void testTag() {
        Value v = tags("foo");
        assertEquals("{foo}", v.toString());
        v = tags("foo", "bar");
        assertEquals("{foo | bar}", v.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTag() {
        tags();
    }

    @Test
    public void testRange() {
        Value v = between(1, 10);
        assertEquals("[1 10]", v.toString());
        v = between(1, 10).inclusiveMax(false);
        assertEquals("[1 (10]", v.toString());
        v = between(1, 10).inclusiveMin(false);
        assertEquals("[(1 10]", v.toString());
        
        v = between(1.0, 10.1);
        assertEquals("[1.0 10.1]", v.toString());
        v = between(-1.0, 10.1).inclusiveMax(false);
        assertEquals("[-1.0 (10.1]", v.toString());
        v = between(-1.1, 150.61).inclusiveMin(false);
        assertEquals("[(-1.1 150.61]", v.toString());

        // le, gt, etc.
        // le, gt, etc.
        assertEquals("[42 42]", eq(42).toString());
        assertEquals("[-inf (42]", lt(42).toString());
        assertEquals("[-inf 42]", le(42).toString());
        assertEquals("[(-42 inf]", gt(-42).toString());
        assertEquals("[42 inf]", ge(42).toString());
        
        assertEquals("[42.0 42.0]", eq(42.0).toString());
        assertEquals("[-inf (42.0]", lt(42.0).toString());
        assertEquals("[-inf 42.0]", le(42.0).toString());
        assertEquals("[(42.0 inf]", gt(42.0).toString());
        assertEquals("[42.0 inf]", ge(42.0).toString());
        
        assertEquals("[(1587058030 inf]", gt(1587058030).toString());

        // string value
        assertEquals("s", value("s").toString());

        // Geo value
        assertEquals("[1.0 2.0 3.0 km]",
                new GeoValue(1.0, 2.0, 3.0, GeoValue.Unit.KILOMETERS).toString());
    }

    @Test
    public void testIntersectionBasic() {
        Node n = intersect().add("name", "mark");
        assertEquals("@name:mark", n.toString());

        n = intersect().add("name", "mark", "dvir");
        assertEquals("@name:(mark dvir)", n.toString());
        
        n = intersect().add("name", Arrays.asList(Values.value("mark"), Values.value("shay")));
        assertEquals("@name:(mark shay)", n.toString());
        
        n = intersect("name", "meir");
        assertEquals("@name:meir", n.toString());
        
        n = intersect("name", Values.value("meir"), Values.value("rafi"));
        assertEquals("@name:(meir rafi)", n.toString());
    }

    @Test
    public void testIntersectionNested() {
        Node n = intersect().
                add(union("name", value("mark"), value("dvir"))).
                add("time", between(100, 200)).
                add(disjunct("created", lt(1000)));
        assertEquals("(@name:(mark|dvir) @time:[100 200] -@created:[-inf (1000])", n.toString());
    }
    
    @Test
    public void testOptional() {
        Node n = optional("name", tags("foo", "bar"));
        assertEquals("~@name:{foo | bar}", n.toString());
        
        n = optional(n, n);
        assertEquals("~(~@name:{foo | bar} ~@name:{foo | bar})", n.toString());
    }

    @Test
    public void testAggregation() {
        assertEquals("*", new AggregationRequest().getArgsString());
        
        AggregationRequest r1 = new AggregationRequest()
            .groupBy("@actor", count().as("cnt"))
            .sortBy(desc("@cnt"));
        assertEquals("* GROUPBY 1 @actor REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC", r1.getArgsString());
        
        Group group = new Group("@brand")
            .reduce(quantile("@price", 0.50).as("q50"))
            .reduce(quantile("@price", 0.90).as("q90"))
            .reduce(quantile("@price", 0.95).as("q95"))
            .reduce(avg("@price"))
            .reduce(count().as("count"));
        AggregationRequest r2 = new AggregationRequest()
            .groupBy(group)
            .sortByDesc("@count")
            .limit(10);
        assertEquals("* GROUPBY 1 @brand REDUCE QUANTILE 2 @price 0.5 AS q50 REDUCE QUANTILE 2 @price 0.9 AS q90 REDUCE QUANTILE 2 @price 0.95 AS q95 REDUCE AVG 1 @price REDUCE COUNT 0 AS count LIMIT 0 10 SORTBY 2 @count DESC",
                r2.getArgsString());
        
        
        AggregationRequest r3 = new AggregationRequest()
            .load("@count")
            .apply("@count%1000", "thousands")
            .sortBy(desc("@count"))
            .limit(0, 2);
        assertEquals("* LOAD 1 @count APPLY @count%1000 AS thousands SORTBY 2 @count DESC LIMIT 0 2", r3.getArgsString());

        AggregationRequest r4 = new AggregationRequest()
            .load(FieldName.of("@fakecount").as("count"))
            .apply("@count%1000", "thousands")
            .sortBy(desc("@count"))
            .limit(0, 2);
        assertEquals("* LOAD 3 @fakecount AS count APPLY @count%1000 AS thousands SORTBY 2 @count DESC LIMIT 0 2", r4.getArgsString());
    }
    
    @Test
    public void testAggregationBuilder() {
        assertEquals("*", new AggregationBuilder().getArgsString());
        
        AggregationBuilder r1 = new AggregationBuilder()
            .groupBy("@actor", count().as("cnt"))
            .sortBy(desc("@cnt"));
        assertEquals("* GROUPBY 1 @actor REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC", r1.getArgsString());
        
        Group group = new Group("@brand")
            .reduce(quantile("@price", 0.50).as("q50"))
            .reduce(quantile("@price", 0.90).as("q90"))
            .reduce(quantile("@price", 0.95).as("q95").setAliasAsField())
            .reduce(avg("@price"))
            .reduce(count().as("count"));
        AggregationBuilder r2 = new AggregationBuilder()
            .groupBy(group)
            .limit(10)
            .sortByDesc("@count");
        
        assertEquals("* GROUPBY 1 @brand REDUCE QUANTILE 2 @price 0.5 AS q50 REDUCE QUANTILE 2 @price 0.9 AS q90 REDUCE QUANTILE 2 @price 0.95 AS @price REDUCE AVG 1 @price REDUCE COUNT 0 AS count LIMIT 0 10 SORTBY 2 @count DESC",
                r2.getArgsString());

        AggregationBuilder r3 = new AggregationBuilder()
                .load("@count")
                .apply("@count%1000", "thousands")
                .sortBy(desc("@count"))
                .limit(0, 2);
        assertEquals("* LOAD 1 @count APPLY @count%1000 AS thousands SORTBY 2 @count DESC LIMIT 0 2", r3.getArgsString());

        AggregationBuilder r4 = new AggregationBuilder()
                .load(FieldName.of("@fakecount").as("count"))
                .apply("@count%1000", "thousands")
                .sortBy(desc("@count"))
                .limit(0, 2);
        assertEquals("* LOAD 3 @fakecount AS count APPLY @count%1000 AS thousands SORTBY 2 @count DESC LIMIT 0 2", r4.getArgsString());

        AggregationBuilder r5 = new AggregationBuilder()
            .groupBy("@actor", 
                count_distinct("@show"), 
                count_distinctish("@price"),
                sum("@count"),
                min("@age"),
                max("@year"),
                stddev("@time"),
                first_value("@name"),
                to_list("@screen"),
                random_sample("@joke", 5))
            .sortBy(desc("@cnt"));
        assertEquals("* GROUPBY 1 @actor "
            + "REDUCE COUNT_DISTINCT 1 @show "
            + "REDUCE COUNT_DISTINCTISH 1 @price "
            + "REDUCE SUM 1 @count "
            + "REDUCE MIN 1 @age "
            + "REDUCE MAX 1 @year "
            + "REDUCE STDDEV 1 @time "
            + "REDUCE FIRST_VALUE 1 @name "
            + "REDUCE TOLIST 1 @screen "
            + "REDUCE RANDOM_SAMPLE 2 @joke 5 "
            + "SORTBY 2 @cnt DESC", 
            r5.getArgsString());
    }
    
}
