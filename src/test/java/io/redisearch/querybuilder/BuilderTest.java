package io.redisearch.querybuilder;

import io.redisearch.aggregation.AggregationRequest;
import org.junit.Test;

import static io.redisearch.aggregation.SortedField.desc;
import static io.redisearch.aggregation.reducers.Reducers.avg;
import static io.redisearch.aggregation.reducers.Reducers.count;
import static io.redisearch.aggregation.reducers.Reducers.quantile;
import static io.redisearch.querybuilder.QueryBuilder.*;
import static io.redisearch.querybuilder.Values.*;
import static junit.framework.TestCase.assertEquals;

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
        assertEquals("[1.0 10.0]", v.toString());
        v = between(1, 10).inclusiveMax(false);
        assertEquals("[1.0 (10.0]", v.toString());
        v = between(1, 10).inclusiveMin(false);
        assertEquals("[(1.0 10.0]", v.toString());

        // le, gt, etc.
        assertEquals("[42.0 42.0]", eq(42).toString());
        assertEquals("[-inf (42.0]", lt(42).toString());
        assertEquals("[-inf 42.0]", le(42).toString());
        assertEquals("[(42.0 inf]", gt(42).toString());
        assertEquals("[42.0 inf]", ge(42).toString());

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
    }

    @Test
    public void testIntersectionNested() {
        Node n = intersect().
                add(union("name", value("mark"), value("dvir"))).
                add("time", between(100, 200)).
                add(disjunct("created", lt(1000)));
        assertEquals("(@name:(mark|dvir) @time:[100.0 200.0] -@created:[-inf (1000.0])", n.toString());
    }

    @Test
    public void testAggregation() {
        assertEquals("*", new AggregationRequest().getArgsString());
        AggregationRequest r = new AggregationRequest().
                groupBy("@actor", count().as("cnt")).
                sortBy(desc("@cnt"));
        assertEquals("* GROUPBY 1 @actor REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC", r.getArgsString());

        r = new AggregationRequest().groupBy("@brand",
                quantile("@price", 0.50).as("q50"),
                quantile("@price", 0.90).as("q90"),
                quantile("@price", 0.95).as("q95"),
                avg("@price"),
                count().as("count")).
                sortByDesc("@count").
                limit(10);
        assertEquals("* GROUPBY 1 @brand REDUCE QUANTILE 2 @price 0.5 AS q50 REDUCE QUANTILE 2 @price 0.9 AS q90 REDUCE QUANTILE 2 @price 0.95 AS q95 REDUCE AVG 1 @price REDUCE COUNT 0 AS count LIMIT 0 10 SORTBY 2 @count DESC",
                r.getArgsString());
    }
}
