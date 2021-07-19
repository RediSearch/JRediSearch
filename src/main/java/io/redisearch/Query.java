package io.redisearch;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

/**
 * Query represents query parameters and filters to load results from the engine
 */
public class Query {


    /**
     * Filter represents a filtering rules in a query
     */
	public abstract static class Filter {

        public final String property;

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
        private final boolean exclusiveMin;
        private final double max;
        private final boolean exclusiveMax;

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

        private byte[] formatNum(double num, boolean exclude) {
            if (num == Double.POSITIVE_INFINITY) { 
                return Keywords.POSITIVE_INFINITY.getRaw();
            }
            if (num == Double.NEGATIVE_INFINITY) {
              return Keywords.NEGATIVE_INFINITY.getRaw();
            }
            
            return exclude ?  SafeEncoder.encode("(" + num)  : Protocol.toByteArray(num);
        }

        @Override
        public void serializeRedisArgs(List<byte[]> args) {
            args.add(Keywords.FILTER.getRaw());
            args.add(SafeEncoder.encode(property));
            args.add(formatNum(min, exclusiveMin));
            args.add(formatNum(max, exclusiveMax));
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
            args.add(Keywords.GEOFILTER.getRaw());
            args.add(SafeEncoder.encode(property));
            args.add(Protocol.toByteArray(lon));
            args.add(Protocol.toByteArray(lat));
            args.add(Protocol.toByteArray(radius));
            args.add(SafeEncoder.encode(unit));
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
        private final String open;
        private final String close;
        public HighlightTags(String open, String close) {
            this.open = open;
            this.close = close;
        }
    }

    /**
     * The query's filter list. We only support AND operation on all those filters
     */
    protected final List<Filter> _filters = new LinkedList<>();

    /**
     * The textual part of the query
     */
    protected final String _queryString;

    /**
     * The sorting parameters
     */
    protected final Paging _paging = new Paging(0, 10);

    protected boolean _verbatim = false;
    protected boolean _noContent = false;
    protected boolean _noStopwords = false;
    protected boolean _withScores = false;
    protected boolean _withPayloads = false;
    protected String _language = null;
    protected String[] _fields = null;
    protected String[] _keys = null;
    protected String[] _returnFields = null;
    private FieldName[] returnFieldNames = null;
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
    protected String _scorer = null;

    public Query() {
        this("*");
    }

    /**
     * Create a new index
     *
     * @param queryString the textual part of the query
     */
    public Query(String queryString) {
        _queryString = queryString;
    }

    public void serializeRedisArgs(List<byte[]> args) {
        args.add(SafeEncoder.encode(_queryString));

        if (_verbatim) {
            args.add(Keywords.VERBATIM.getRaw());
        }
        if (_noContent) {
            args.add(Keywords.NOCONTENT.getRaw());
        }
        if (_noStopwords) {
            args.add(Keywords.NOSTOPWORDS.getRaw());
        }
        if (_withScores) {
            args.add(Keywords.WITHSCORES.getRaw());
        }
        if (_withPayloads) {
            args.add(Keywords.WITHPAYLOADS.getRaw());
        }
        if (_language != null) {
            args.add(Keywords.LANGUAGE.getRaw());
            args.add(SafeEncoder.encode(_language));
        }
        
        if (_scorer != null) {
          args.add(Keywords.SCORER.getRaw());
          args.add(SafeEncoder.encode(_scorer));
        }
        
        if (_fields != null && _fields.length > 0) {
            args.add(Keywords.INFIELDS.getRaw());
            args.add(Protocol.toByteArray(_fields.length));
            for (String f : _fields) {
                args.add(SafeEncoder.encode(f));
            }
        }

        if (_sortBy != null) {
            args.add(Keywords.SORTBY.getRaw());
            args.add(SafeEncoder.encode(_sortBy));
            args.add((_sortAsc ? Keywords.ASC : Keywords.DESC).getRaw());
        }

        if (_payload != null) {
            args.add(Keywords.PAYLOAD.getRaw());
            args.add(_payload);
        }

        if (_paging.offset != 0 || _paging.num != 10) {
            args.addAll(Arrays.asList(Keywords.LIMIT.getRaw(),
                Protocol.toByteArray(_paging.offset),
                Protocol.toByteArray(_paging.num)
            ));
        }

        if (!_filters.isEmpty()) {
            for (Filter f : _filters) {
                f.serializeRedisArgs(args);
            }
        }

        if (wantsHighlight) {
            args.add(Keywords.HIGHLIGHT.getRaw());
            if (highlightFields != null) {
                args.add(Keywords.FIELDS.getRaw());
                args.add(Protocol.toByteArray(highlightFields.length));
                for (String s : highlightFields) {
                    args.add(SafeEncoder.encode(s));
                }
            }
            if (highlightTags != null) {
                args.add(Keywords.TAGS.getRaw());
                for (String t : highlightTags) {
                    args.add(SafeEncoder.encode(t));
                }
            }
        }
        if (wantsSummarize) {
            args.add(Keywords.SUMMARIZE.getRaw());
            if (summarizeFields != null) {
                args.add(Keywords.FIELDS.getRaw());
                args.add(Protocol.toByteArray(summarizeFields.length));
                for (String s: summarizeFields) {
                    args.add(SafeEncoder.encode(s));
                }
            }
            if (summarizeNumFragments != -1) {
                args.add(Keywords.FRAGS.getRaw());
                args.add(Protocol.toByteArray(summarizeNumFragments));
            }
            if (summarizeFragmentLen != -1) {
                args.add(Keywords.LEN.getRaw());
                args.add(Protocol.toByteArray(summarizeFragmentLen));
            }
            if (summarizeSeparator != null) {
                args.add(Keywords.SEPARATOR.getRaw());
                args.add(SafeEncoder.encode(summarizeSeparator));
            }
        }
        
        if (_keys != null && _keys.length > 0) {
          args.add(Keywords.INKEYS.getRaw());
          args.add(Protocol.toByteArray(_keys.length));
          for (String f : _keys) {
              args.add(SafeEncoder.encode(f));
          }
        }
        
        if (_returnFields != null && _returnFields.length > 0) {
          args.add(Keywords.RETURN.getRaw());
          args.add(Protocol.toByteArray( _returnFields.length));
          for (String f : _returnFields) {
              args.add(SafeEncoder.encode(f));
          }
        } else if (returnFieldNames != null && returnFieldNames.length > 0) {
            args.add(Keywords.RETURN.getRaw());
            final int returnCountIndex = args.size();
            args.add(null); // holding a place for setting the total count later.
            int returnCount = 0;
            for (FieldName fn : returnFieldNames) {
                returnCount += fn.addCommandBinaryArguments(args);
            }
            args.set(returnCountIndex, Protocol.toByteArray(returnCount));
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

    public boolean getNoContent() {
        return _noContent;
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

    public boolean getWithScores() {
        return _withScores;
    }
    
    /**
     * Set the query to return a factored score for each results. This is useful to merge results from multiple queries.
     * @return the query object itself
     */
    public Query setWithScores() {
        this._withScores = true;
        return this;
    }


    public boolean getWithPayloads() {
        return _withPayloads;
    }
    
    /**
     * @deprecated {@link #setWithPayload()}
     */
    @Deprecated
    public Query setWithPaload() {
      return this.setWithPayload();
    }
    
    /**
     * Set the query to return object payloads, if any were given
     * 
     * @return the query object itself
     * */
    public Query setWithPayload() {
        this._withPayloads = true;
        return this;
    }

    /**
     * Set the query language, for stemming purposes
     * @param language a language. 
     * 
     * @return the query object itself
     * 
     * @see http://redisearch.io for documentation on languages and stemming
     */
    public Query setLanguage(String language) {
        this._language = language;
        return this;
    }

    /**
     * Set the query custom scorer
     * @param scorer a custom scorer. 
     * 
     * @return the query object itself
     * 
     * @see http://redisearch.io for documentation on extending RediSearch
     */
    public Query setScorer(String scorer) {
        this._scorer = scorer;
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
    
    /**
     * Limit the query to results that are limited to a specific set of keys
     * @param keys a list of TEXT fields in the schemas
     * @return the query object itself
     */
    public Query limitKeys(String... keys) {
        this._keys = keys;
        return this;
    }

    /**
     * Result's projection - the fields to return by the query
     * @param fields a list of TEXT fields in the schemas
     * @return the query object itself
     */
    public Query returnFields(String... fields) {
        this._returnFields = fields;
        this.returnFieldNames = null;
        return this;
    }

    /**
     * Result's projection - the fields to return by the query
     * @param fields a list of TEXT fields in the schemas
     * @return the query object itself
     */
    public Query returnFields(FieldName... fields) {
        this.returnFieldNames = fields;
        this._returnFields = null;
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
     * Set the query to be sorted by a Sortable field defined in the schema
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
