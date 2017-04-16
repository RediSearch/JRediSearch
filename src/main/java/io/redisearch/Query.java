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

    /**
     * NumericFilter wraps a range filter on a numeric field. It can be inclusive or exclusive
     */
    public static class NumericFilter extends Filter {

        private final double min;
        boolean exclusiveMin;
        private final double max;
        boolean exclusiveMax;

        public NumericFilter(String property, double min, boolean exclusiveMin, double max, boolean exclusiveMax) {
            super(property);
            this.min = min;
            this.max = max;
            this.exclusiveMax = exclusiveMax;
            this.exclusiveMin = exclusiveMin;
        }

        public NumericFilter(String property, double min, double max) {
            this(property, min, false, max, false);
        }

        private String formatNum(double num, boolean exclude) {
            if (num == Double.POSITIVE_INFINITY) {
                return "+inf";
            }
            if (num == Double.NEGATIVE_INFINITY) {
                return "-inf";
            }
            return String.format("%s%f", exclude ? "(" : "", num);
        }

        @Override
        public void serializeRedisArgs(List<String> args) {
            args.addAll(Arrays.asList("FILTER", property,
                    formatNum(min, exclusiveMin),
                    formatNum(max, exclusiveMax)));

        }
    }

    /**
     * GeoFilter encapsulates a radius filter on a geographical indexed fields
     */
    public static class GeoFilter extends Filter {

        public static final String KILOMETERS = "km";
        public static final String METERS = "m";
        public static final String FEET = "ft";
        public static final String MILES = "mi";

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

    public boolean getNoContent() {
        return _noContent;
    }

    public boolean getWithScores() {
        return _withScores;
    }

    public boolean getWithPayloads() {
        return _withPayloads;
    }

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
            args.add("INFIELDS");
            args.add(String.format("%d", _fields.length));
            args.addAll(Arrays.asList(_fields));
        }

        if (_paging.offset != 0 || _paging.num != 10) {
            args.addAll(Arrays.asList("LIMIT",
                    Integer.toString(_paging.offset),
                    Integer.toString(_paging.num)
            ));
        }

        if (_filters != null && _filters.size() > 0) {
            for (Filter f : _filters) {
                f.serializeRedisArgs(args);
            }
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

    /**
     * Add a filter to the query's filter list
     * @param f either a numeric or geo filter object
     * @return the query itself
     */
    public Query addFilter(Filter f) {
        _filters.add(f);
        return this;
    }

    /**
     * Set the query to verbatim mode, disabling stemming and query expansion
     * @return the query object
     */
    public Query setVerbatim() {
        this._verbatim = true;
        return this;
    }

    /**
     * Set the query not to return the contents of documents, and rather just return the ids
     * @return the query itself
     */
    public Query setNoContent() {
        this._noContent = true;
        return this;
    }

    /**
     * Set the query not to filter for stopwords. In general this should not be used
     * @return the query object
     */
    public Query setNoStopwords() {
        this._noStopwords = true;
        return this;
    }

    /**
     * Set the query to return a factored score for each results. This is useful to merge results from multiple queries.
     * @return the query object itself
     */
    public Query setWithScores() {
        this._withScores = true;
        return this;
    }

    /** Set the query to return object payloads, if any were given */
    public Query setWithPaload() {
        this._withPayloads = true;
        return this;
    }

    /**
     * Set the query language, for stemming purposes
     * @param language a language. see http://redisearch.io for documentation on languages and stemming
     * @return
     */
    public Query setLanguage(String language) {
        this._language = language;
        return this;
    }

    /**
     * Limit the query to results that are limited to a specific set of fields
     * @param fields a list of TEXT fields in the schemas
     * @return the query object itself
     */
    public Query limitFields(String... fields) {
        this._fields = fields;
        return this;
    }
}
