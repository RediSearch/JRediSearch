package io.redisearch;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Query represents query parameters and filters to load results from the engine
 */
public class Query {


    /**
     * Filter represents a filtering rules in a query
     */
    private static abstract class Filter {

        public String property;

        public abstract void serializeRedisArgs(List<String> args);

        public Filter(String property) {
            this.property = property;
        }

    }

    public static class NumericFilter extends Filter {

        private final double min;
        private final double max;

        public NumericFilter(String property, double min, double max) {
            super(property);
            this.min = min;
            this.max = max;
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.addAll(Arrays.asList("FILTER", property, Double.toString(min), Double.toString(max)));
        }
    }

    public static class GeoFilter extends Filter {

        static final String KILOMETERS = "km";
        static final String MEETERS = "m";
        static final String FEET = "ft";
        static final String MILES = "mi";

        private final double lon;
        private final double lat;
        private final double radius;
        private final String unit;

        public GeoFilter(String property, double lon, double lat, double radius, String unit) {
            super(property);
            this.lon = lon;
            this.lat = lat;
            this.radius = radius;
            this.unit = unit;
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.addAll(Arrays.asList("GEOFILTER", property, Double.toString(lon),
                    Double.toString(lat),
                    Double.toString(radius),
                    unit));
        }
    }

    public static class Paging {
        int offset;
        int num;

        public Paging(int offset, int num) {
            this.offset = offset;
            this.num = num;
        }
    }

    /**
     * The query's filter list. We only support AND operation on all those filters
     */
    protected List<Filter> _filters = new LinkedList<>();


    /**
     * The textual part of the query
     */
    protected String _queryString;

    /**
     * The sorting parameters
     */
    protected Paging _paging = new Paging(0, 10);

    protected boolean _verbatim = false;
    protected boolean _noContent = false;
    protected boolean _noStopwords = false;
    protected boolean _withScores = false;
    protected boolean _withPayloads = false;
    protected String _language = null;
    protected String[] _fields = null;

    /**
     * Create a new index
     */
    public Query(String queryString) {

        _queryString = queryString;
    }

    public void serializeRedisArgs(List<String> args) {
        args.add(_queryString);

        if (_verbatim) {
            args.add("VERBATIM");
        }
        if (_noContent) {
            args.add("NOCONTENT");
        }
        if (_noStopwords) {
            args.add("NOSTOPWORDS");
        }
        if (_withScores) {
            args.add("WITHSCORES");
        }
        if (_withPayloads) {
            args.add("WITHPAYLOADS");
        }
        if (_language != null) {
            args.add("LANGUAGE");
            args.add(_language);
        }
        if (_fields != null && _fields.length > 0) {
            args.addAll(Arrays.asList(_fields));
        }

        if (_paging.offset != 0 || _paging.num != 10) {
            args.addAll(Arrays.asList("LIMIT",
                    Integer.toString(_paging.offset),
                    Integer.toString(_paging.num)
            ));
        }
    }

    /**
     * Limit the results to a certain offset and limit
     *
     * @param offset the first result to show, zero based indexing
     * @param limit  how many results we want to show
     * @return the query itself, for builder-style syntax
     */
    public Query limit(Integer offset, Integer limit) {
        _paging.offset = offset;
        _paging.num = limit;
        return this;
    }

    public Query addFilter(Filter f) {
        _filters.add(f);
        return this;
    }

    public Query setVerbatim() {
        this._verbatim = true;
        return this;
    }

    public Query setNoContent() {
        this._noContent = true;
        return this;
    }

    public Query setNoStopword() {
        this._noStopwords = true;
        return this;
    }

    public Query setWithScores() {
        this._withScores = true;
        return this;
    }

    public Query setWithPaload() {
        this._withPayloads = true;
        return this;
    }

    public Query setLanguage(String language) {
        this._language = language;
        return this;
    }

    public Query limitFields(String... fields) {
        this._fields = fields;
        return this;
    }
}
