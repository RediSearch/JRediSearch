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

        public abstract void serializeRedisArgs(List<byte[]> args);

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
        public void serializeRedisArgs(List<byte[]> args) {
            args.addAll(Arrays.asList("FILTER".getBytes(), property.getBytes(),
                    formatNum(min, exclusiveMin).getBytes(),
                    formatNum(max, exclusiveMax).getBytes()));

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
        public void serializeRedisArgs(List<byte[]> args) {
            args.addAll(Arrays.asList("GEOFILTER".getBytes(),
                    property.getBytes(), Double.toString(lon).getBytes(),
                    Double.toString(lat).getBytes(),
                    Double.toString(radius).getBytes(),
                    unit.getBytes()));
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

    public static class HighlightTags {
        String open;
        String close;
        public HighlightTags(String open, String close) {
            this.open = open;
            this.close = close;
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
    protected String[] highlightFields = null;
    protected String[] summarizeFields = null;
    protected String[] highlightTags = null;
    protected String summarizeSeparator = null;
    protected int summarizeNumFragments = -1;
    protected int summarizeFragmentLen = -1;
    protected byte []_payload = null;
    protected String _sortBy = null;
    protected boolean _sortAsc = true;
    protected boolean wantsHighlight = false;
    protected boolean wantsSummarize = false;

    /**
     * Create a new index
     */
    public Query(String queryString) {

        _queryString = queryString;
    }

    public void serializeRedisArgs(List<byte[]> args) {
        args.add(_queryString.getBytes());

        if (_verbatim) {
            args.add("VERBATIM".getBytes());
        }
        if (_noContent) {
            args.add("NOCONTENT".getBytes());
        }
        if (_noStopwords) {
            args.add("NOSTOPWORDS".getBytes());
        }
        if (_withScores) {
            args.add("WITHSCORES".getBytes());
        }
        if (_withPayloads) {
            args.add("WITHPAYLOADS".getBytes());
        }
        if (_language != null) {
            args.add("LANGUAGE".getBytes());
            args.add(_language.getBytes());
        }
        if (_fields != null && _fields.length > 0) {
            args.add("INFIELDS".getBytes());
            args.add(String.format("%d", _fields.length).getBytes());
            for (String f : _fields) {
                args.add(f.getBytes());
            }

        }

        if (_sortBy != null) {
            args.add("SORTBY".getBytes());
            args.add(_sortBy.getBytes());
            args.add((_sortAsc ? "ASC" : "DESC").getBytes());
        }

        if (_payload != null) {
            args.add("PAYLOAD".getBytes());
            args.add(_payload);
        }

        if (_paging.offset != 0 || _paging.num != 10) {
            args.addAll(Arrays.asList("LIMIT".getBytes(),
                    Integer.toString(_paging.offset).getBytes(),
                    Integer.toString(_paging.num).getBytes()
            ));
        }

        if (_filters != null && _filters.size() > 0) {
            for (Filter f : _filters) {
                f.serializeRedisArgs(args);
            }
        }

        if (wantsHighlight) {
            args.add("HIGHLIGHT".getBytes());
            if (highlightFields != null) {
                args.add("FIELDS".getBytes());
                args.add(Integer.toString(highlightFields.length).getBytes());
                for (String s : highlightFields) {
                    args.add(s.getBytes());
                }
            }
            if (highlightTags != null) {
                args.add("TAGS".getBytes());
                for (String t : highlightTags) {
                    args.add(toString().getBytes());
                }
            }
        }
        if (wantsSummarize) {
            args.add("SUMMARIZE".getBytes());
            if (summarizeFields != null) {
                args.add("FIELDS".getBytes());
                args.add(Integer.toString(summarizeFields.length).getBytes());
                for (String s: summarizeFields) {
                    args.add(s.getBytes());
                }
            }
            if (summarizeNumFragments != -1) {
                args.add("FRAGS".getBytes());
                args.add(Integer.toString(summarizeNumFragments).getBytes());
            }
            if (summarizeFragmentLen != -1) {
                args.add("LEN".getBytes());
                args.add(Integer.toString(summarizeFragmentLen).getBytes());
            }
            if (summarizeSeparator != null) {
                args.add("SEPARATOR".getBytes());
                args.add(summarizeSeparator.getBytes());
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

    /* Set the query payload to be evaluated by the scoring function */
    public Query setPayload(byte []payload) {
        _payload = payload;
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

    public Query highlightFields(HighlightTags tags, String... fields) {
        if (fields == null || fields.length > 0) {
            highlightFields = fields;
        }
        if (tags != null) {
            highlightTags = new String[]{tags.open, tags.close};
        } else {
            highlightTags = null;
        }
        wantsHighlight = true;
        return this;
    }

    public Query highlightFields(String... fields) {
        return highlightFields(null, fields);
    }

    public Query summarizeFields(int contextLen, int fragmentCount, String separator, String ... fields) {
        if (fields == null || fields.length > 0) {
            summarizeFields = fields;
        }
        summarizeFragmentLen = contextLen;
        summarizeNumFragments = fragmentCount;
        summarizeSeparator = separator;
        wantsSummarize = true;
        return this;
    }

    public Query summarizeFields(String... fields) {
        return summarizeFields(-1, -1, null, fields);
    }

    /**
     * Set the query to be sorted by a sortable field defined in the schem
     * @param field the sorting field's name
     * @param ascending if set to true, the sorting order is ascending, else descending
     * @return the query object itself
     */
    public Query setSortBy(String field, boolean ascending) {
        _sortBy = field;
        _sortAsc = ascending;
        return this;
    }
}
