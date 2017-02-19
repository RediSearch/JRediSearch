package io.redisearch;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by dvirsky on 19/02/17.
 */
public class QueryTest {

    Query query;
    @Before
    public void setUp() throws Exception {
        query = new Query("hello world");
    }

    @Test
    public void getNoContent() throws Exception {
        assertFalse(query.getNoContent());
        assertEquals(query, query.setNoContent());
        assertTrue(query.getNoContent());
    }

    @Test
    public void getWithScores() throws Exception {
        assertFalse(query.getWithScores());
        assertEquals(query, query.setWithScores());
        assertTrue(query.getWithScores());
    }

    @Test
    public void serializeRedisArgs() throws Exception {
        query.setNoContent().setLanguage("xx").setNoStopwords().setVerbatim().setWithPaload().setWithScores();

        ArrayList<String> args = new ArrayList<>(1);
        query.serializeRedisArgs(args);

        assertEquals(8, args.size());
        assertEquals(query._queryString, args.get(0));
        assertTrue(args.contains("xx"));
        assertTrue(args.contains("NOSTOPWORDS"));
        assertTrue(args.contains("VERBATIM"));
        assertTrue(args.contains("WITHPAYLOADS"));
        assertTrue(args.contains("WITHSCORES"));
    }

    @Test
    public void limit() throws Exception {
        assertEquals(0, query._paging.offset);
        assertEquals(10, query._paging.num);
        assertEquals(query, query.limit(1, 30));
        assertEquals(1, query._paging.offset);
        assertEquals(30, query._paging.num);

    }

    @Test
    public void addFilter() throws Exception {
        assertEquals(0, query._filters.size());
        Query.NumericFilter f = new Query.NumericFilter("foo", 0, 100);
        assertEquals(query, query.addFilter(f));
        assertEquals(f, query._filters.get(0));
    }

    @Test
    public void setVerbatim() throws Exception {
        assertFalse(query._verbatim);
        assertEquals(query, query.setVerbatim());
        assertTrue(query._verbatim);
    }


    @Test
    public void setNoStopwords() throws Exception {
        assertFalse(query._noStopwords);
        assertEquals(query, query.setNoStopwords());
        assertTrue(query._noStopwords);

    }


    @Test
    public void setLanguage() throws Exception {
        assertEquals(null, query._language);
        assertEquals(query, query.setLanguage("chinese"));
        assertEquals("chinese", query._language);
    }

    @Test
    public void limitFields() throws Exception {

        assertEquals( null, query._fields);
        assertEquals(query, query.limitFields("foo", "bar"));
        assertEquals(2, query._fields.length);

    }

}