package io.redisearch.client;

import io.redisearch.*;
import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.SuggestionOptions.With;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Pool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;

/**
 * Client is the main RediSearch client class, wrapping connection management and all RediSearch commands
 */
public class Client implements io.redisearch.Client {

    private final String indexName;
    private final Pool<Jedis> pool;

    protected Commands.CommandProvider commands;
    
    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param pool jedis connection pool to be used
     */
    public Client(String indexName, Pool<Jedis> pool) {
      this.indexName = indexName;
      this.pool = pool;
      this.commands = new Commands.SingleNodeCommands();
    }
    
    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     */
    public Client(String indexName, String host, int port) {
        this(indexName, host, port, 500, 100);
    }
    
    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     */
    public Client(String indexName, String host, int port, int timeout, int poolSize) {
        this(indexName, host, port, timeout, poolSize, null);
    }

    /**
     * Create a new client to a RediSearch index
     *
     * @param indexName the name of the index we are connecting to or creating
     * @param host      the redis host
     * @param port      the redis pot
     * @param password  the password for authentication in a password protected Redis server
     */
    public Client(String indexName, String host, int port, int timeout, int poolSize, String password) {
        this(indexName, new JedisPool(initPoolConfig(poolSize), host, port, timeout, password));
    }

    /**
     * Create a new client to a RediSearch index with JediSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     *
     * @param indexName  the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels  the set of sentinels monitoring the cluster
     * @param timeout    the timeout in milliseconds
     * @param poolSize   the poolSize of JedisSentinelPool
     * @param password   the password for authentication in a password protected Redis server
     */
    public Client(String indexName, String master, Set<String> sentinels, int timeout, int poolSize, String password) {
        this(indexName,new JedisSentinelPool(master, sentinels, initPoolConfig(poolSize), timeout, password));
    }

    /**
     * Create a new client to a RediSearch index with JediSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     *
     * <p>The Client is initialized with following default values for {@link JedisSentinelPool}
     * <ul><li> password - NULL, no authentication required to connect to Redis Server</li></ul>
     *
     * @param indexName  the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels  the set of sentinels monitoring the cluster
     * @param timeout    the timeout in milliseconds
     * @param poolSize   the poolSize of JedisSentinelPool
     */
    public Client(String indexName, String masterName, Set<String> sentinels, int timeout, int poolSize) {
        this(indexName, masterName, sentinels, timeout, poolSize, null);
    }

    /**
     * Create a new client to a RediSearch index with JediSentinelPool implementation. JedisSentinelPool
     * takes care of reconfiguring the Pool when there is a failover of master node thus providing high
     * availability and automatic failover.
     *
     * <p>The Client is initialized with following default values for {@link JedisSentinelPool}
     * <ul> <li>timeout - 500 mills</li>
     * <li> poolSize - 100 connections</li>
     * <li> password - NULL, no authentication required to connect to Redis Server</li></ul>
     *
     * @param indexName  the name of the index we are connecting to or creating
     * @param masterName the masterName to connect from list of masters monitored by sentinels
     * @param sentinels  the set of sentinels monitoring the cluster
     */
    public Client(String indexName, String masterName, Set<String> sentinels) {
        this(indexName, masterName, sentinels, 500, 100);
    }

    private static void handleListMapping(List<Object> items, KVHandler handler, boolean decode) {
        for (int i = 0; i < items.size(); i += 2) {
            String key = new String((byte[]) items.get(i));
            Object val = items.get(i + 1);
            if (decode && val instanceof byte[]) {
                val = SafeEncoder.encode((byte[]) val);
            }
            handler.apply(key, val);
        }
    }

    Jedis _conn() {
        return pool.getResource();
    }

    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, String... args) {
        BinaryClient client = conn.getClient();
        client.sendCommand(provider, args);
        return client;
    }

    private BinaryClient sendCommand(Jedis conn, ProtocolCommand provider, byte[][] args) {
        BinaryClient client = conn.getClient();
        client.sendCommand(provider, args);
        return client;
    }

    /**
     * Constructs JedisPoolConfig object.
     *
     * @param poolSize size of the JedisPool
     * @return {@link JedisPoolConfig} object with a few default settings
     */
    private static JedisPoolConfig initPoolConfig(int poolSize) {
        JedisPoolConfig conf = new JedisPoolConfig();
        conf.setMaxTotal(poolSize);
        conf.setTestOnBorrow(false);
        conf.setTestOnReturn(false);
        conf.setTestOnCreate(false);
        conf.setTestWhileIdle(false);
        conf.setMinEvictableIdleTimeMillis(60000);
        conf.setTimeBetweenEvictionRunsMillis(30000);
        conf.setNumTestsPerEvictionRun(-1);
        conf.setFairness(true);

        return conf;
    }

    /**
     * Create the index definition in redis
     *
     * @param schema  a schema definition, see {@link Schema}
     * @param options index option flags, see {@link IndexOptions}
     * @return true if successful
     */
    public boolean createIndex(Schema schema, IndexOptions options) {

        ArrayList<String> args = new ArrayList<>();

        args.add(indexName);

        options.serializeRedisArgs(args);

        args.add("SCHEMA");

        for (Schema.Field f : schema.fields) {
            f.serializeRedisArgs(args);
        }

        try (Jedis conn = _conn()) {
            String rep = sendCommand(conn, commands.getCreateCommand(), args.toArray(new String[args.size()]))
                    .getStatusCodeReply();
            return rep.equals("OK");
        }
    }

    /**
     * Search the index
     *
     * @param q a {@link Query} object with the query string and optional parameters
     * @return a {@link SearchResult} object with the results
     */
    public SearchResult search(Query q) {
      return this.search(q, true);
    }
    
    /**
     * Search the index
     *
     * @param q a {@link Query} object with the query string and optional parameters
     * @param decode <code>false</code> - keeps the fields value as byte[] 
     * 
     * @return a {@link SearchResult} object with the results
     */
    public SearchResult search(Query q, boolean decode) {
        ArrayList<byte[]> args = new ArrayList<>(4);
        args.add(SafeEncoder.encode(indexName));
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            List<Object> resp =
                    sendCommand(conn, commands.getSearchCommand(),
                            args.toArray(new byte[args.size()][])).getObjectMultiBulkReply();
            return new SearchResult(resp, !q.getNoContent(), q.getWithScores(), q.getWithPayloads(), decode);
        }
    }

    public AggregationResult aggregate(AggregationRequest q) {
        ArrayList<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(indexName));
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            List<Object> resp = sendCommand(conn, commands.getAggregateCommand(), args.toArray(new byte[args.size()][]))
                    .getObjectMultiBulkReply();
            return new AggregationResult(resp);
        }
    }

    /**
     * Generate an explanatory textual query tree for this query string
     *
     * @param q The query to explain
     * @return A string describing this query
     */
    public String explain(Query q) {
        ArrayList<byte[]> args = new ArrayList<>(4);
        args.add(SafeEncoder.encode(indexName));
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            return sendCommand(conn, commands.getExplainCommand(), args.toArray(new byte[args.size()][])).getStatusCodeReply();
        }
    }

    /**
     * Add a single document to the query
     *
     * @param docId   the id of the document. It cannot belong to a document already in the index unless replace is set
     * @param score   the document's score, floating point number between 0 and 1
     * @param fields  a map of the document's fields
     * @param noSave  if set, we only index the document and do not save its contents. This allows fetching just doc ids
     * @param replace if set, and the document already exists, we reindex and update it
     * @param payload if set, we can save a payload in the index to be retrieved or evaluated by scoring functions on the server
     * @return true on success
     */
    public boolean addDocument(String docId, double score, Map<String, Object> fields, boolean noSave, boolean replace, byte[] payload) {
        return doAddDocument(docId, score, fields, noSave, replace, false, payload);
    }

    private boolean doAddDocument(String docId, double score, Map<String, Object> fields, boolean noSave, boolean replace, boolean partial, byte[] payload) {
        Document doc = new Document(docId, fields, score, payload);
        AddOptions options = new AddOptions().setNosave(noSave);
        if (replace) {
            options.setReplacementPolicy(AddOptions.ReplacementPolicy.FULL);
        }
        if (partial) {
            options.setReplacementPolicy(AddOptions.ReplacementPolicy.PARTIAL);
        }
        return addDocument(doc, options);
    }
    
    /**
     * Add a document to the index
     * 
     * @param doc The document to add
     * @return true on success
     */
    public boolean addDocument(Document doc) {
        return addDocument(doc, new AddOptions());
    }

    /**
     * Add a document to the index
     *
     * @param doc     The document to add
     * @param options Options for the operation
     * @return true on success
     */
    public boolean addDocument(Document doc, AddOptions options) {
        try (Jedis conn = _conn()) {
            return addDocument(doc, options, conn).getStatusCodeReply().equals("OK");
        }
    }
    
    /**
     * see {@link #addDocuments(AddOptions, Document...)}
     */
    public boolean[] addDocuments(Document... docs){
    	return addDocuments(new AddOptions(), docs);
    }
    
    /**
     * Add a batch of documents to the index
     * @param options Options for the operation
     * @param docs The documents to add
     * @return true on success for each document 
     */
    public boolean[] addDocuments(AddOptions options, Document... docs){
    	try (Jedis conn = _conn()) {
	    	for(Document doc : docs) {
	    		addDocument(doc, options, conn);
	    	}
	    	List<Object> objects = conn.getClient().getMany(docs.length);
	    	boolean[] results = new boolean[docs.length];
	    	int i=0;
	    	for(Object obj : objects) {
	    		results[i++] = !(obj instanceof JedisDataException) && 
	    		SafeEncoder.encode((byte[]) obj).equals("OK");
	    	}
	    	return results;
    	}
    }
    
    private BinaryClient addDocument(Document doc, AddOptions options, Jedis conn) {
        ArrayList<byte[]> args = new ArrayList<>();
        args.add(SafeEncoder.encode(indexName));
        args.add(SafeEncoder.encode(doc.getId()));
        args.add(Protocol.toByteArray(doc.getScore()));
        
        if (options.getNosave()) {
            args.add(Keywords.NOSAVE.getRaw());
        }
        if (options.getReplacementPolicy() != AddOptions.ReplacementPolicy.NONE) {
            args.add(Keywords.REPLACE.getRaw());
            if (options.getReplacementPolicy() == AddOptions.ReplacementPolicy.PARTIAL) {
                args.add(Keywords.PARTIAL.getRaw());
            }
        }
        if (options.getLanguage() != null && !options.getLanguage().isEmpty()) {
            args.add(Keywords.LANGUAGE.getRaw());
            args.add(SafeEncoder.encode(options.getLanguage()));
        }
        if (doc.getPayload() != null) {
            args.add(Keywords.PAYLOAD.getRaw());
            args.add(doc.getPayload());
        }

        args.add(Keywords.FIELDS.getRaw());
        for (Map.Entry<String, Object> ent : doc.getProperties()) {
            args.add(SafeEncoder.encode(ent.getKey()));
            Object value = ent.getValue();
            args.add(value instanceof byte[] ?  (byte[])value :  SafeEncoder.encode(value.toString()));
        }

        return sendCommand(conn, commands.getAddCommand(), args.toArray(new byte[args.size()][])); 
    }

    /**
     * replaceDocument is a convenience for calling addDocument with replace=true
     *
     * @param docId
     * @param score
     * @param fields
     * @return true on success
     */
    public boolean replaceDocument(String docId, double score, Map<String, Object> fields) {
        return addDocument(docId, score, fields, false, true, null);
    }

    /**
     * Replace specific fields in a document. Unlike #replaceDocument(), fields not present in the field list
     * are not erased, but retained. This avoids reindexing the entire document if the new values are not
     * indexed (though a reindex will happen
     *
     * @param docId  the id of the document. It cannot belong to a document already in the index unless replace is set
     * @param score  the document's score, floating point number between 0 and 1
     * @param fields a map of the document's fields
     * @return true on success
     */
    public boolean updateDocument(String docId, double score, Map<String, Object> fields) {
        return doAddDocument(docId, score, fields, false, true, true, null);
    }

    /**
     * See {@link #updateDocument(String, double, Map)}
     */
    public boolean addDocument(String docId, double score, Map<String, Object> fields) {
        return this.addDocument(docId, score, fields, false, false, null);
    }

    /**
     * See {@link #updateDocument(String, double, Map)}
     */
    public boolean addDocument(String docId, Map<String, Object> fields) {
        return this.addDocument(docId, 1, fields, false, false, null);
    }

    /**
     * Index a document already in redis as a HASH key.
     *
     * @param docId   the id of the document in redis. This must match an existing, unindexed HASH key
     * @param score   the document's index score, between 0 and 1
     * @param replace if set, and the document already exists, we reindex and update it
     * @return true on success
     */
    public boolean addHash(String docId, double score, boolean replace) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName, docId, Double.toString(score)));

        if (replace) {
            args.add("REPLACE");
        }

        try (Jedis conn = _conn()) {
            String resp = sendCommand(conn, commands.getAddHashCommand(), args.toArray(new String[args.size()])).getStatusCodeReply();
            return resp.equals("OK");
        }
    }

    /**
     * Get the index info, including memory consumption and other statistics.
     * TODO: Make a class for easier access to the index properties
     *
     * @return a map of key/value pairs
     */
    public Map<String, Object> getInfo() {
        List<Object> res;
        try (Jedis conn = _conn()) {
            res = sendCommand(conn, commands.getInfoCommand(), this.indexName).getObjectMultiBulkReply();
        }

        Map<String, Object> info = new HashMap<>();
        handleListMapping(res, info::put, true /*decode*/);
        return info;
    }
    /**
     * Delete a documents from the index
     *
     * @param deleteDocuments  if <code>true</code> also deletes the actual document ifs it is in the index
     * @param docIds the document's ids
     * @return true on success for each document if it has been deleted, false if it did not exist
     */
    public boolean[] deleteDocuments(boolean deleteDocuments, String... docIds) {
    	try (Jedis conn = _conn()) {
	    	for(String docId : docIds) {
	    		deleteDocument(docId, deleteDocuments, conn);
	    	}
	    	List<Object> objects = conn.getClient().getMany(docIds.length);
	    	boolean[] results = new boolean[docIds.length];
	    	int i=0;
	    	for(Object obj : objects) {
	    		results[i++] = !(obj instanceof JedisDataException) && 
	    		((Long) obj) == 1L;
	    	}
	    	return results;
    	}
    }
    
    /**
     * Delete a document from the index (doesn't delete the document).
     *
     * @param docId the document's id
     * @return true if it has been deleted, false if it did not exist
     * 
     * @see #deleteDocument(String, boolean) 
     */
    public boolean deleteDocument(String docId) {
    	return deleteDocument(docId, false);
    }
    
    /**
     * Delete a document from the index.
     *
     * @param docId the document's id
     * @param deleteDocument if <code>true</code> also deletes the actual document if it is in the index
     * @return true if it has been deleted, false if it did not exist
     */
    public boolean deleteDocument(String docId, boolean deleteDocument) {
        try (Jedis conn = _conn()) {        	
        	return deleteDocument(docId, deleteDocument, conn).getIntegerReply() == 1;
        }
    }
    
    
    /**
     * Delete a document from the index.
     *
     * @param docId the document's id
     * @param deleteDocument if <code>true</code> also deletes the actual document if it is in the index
     * @param conn client connection to be used
     * @return reference to the {@link BinaryClient} too allow chaining 
     */
    private BinaryClient deleteDocument(String docId, boolean deleteDocument, Jedis conn) {
    	if(deleteDocument) {
    		return sendCommand(conn, commands.getDelCommand(), this.indexName, docId, DELETE_DOCUMENT);
    	} else {
    		return sendCommand(conn, commands.getDelCommand(), this.indexName, docId);
    	}
    }

    /**
     * Get a document from the index
     *
     * @param docId The document ID to retrieve
     * 
     * @return The document as stored in the index. If the document does not exist, null is returned.
     * Decode values by default as {@link String}
     * 
     * @see #getDocument(String, boolean)
     */
    public Document getDocument(String docId) {
      return this.getDocument(docId, true);
    }

    /**
     * Get a document from the index
     *
     * @param docId The document ID to retrieve
     * @param decode <code>false</code> - keeps the fields value as byte[] 
     * @return The document as stored in the index. If the document does not exist, null is returned.
     */
    public Document getDocument(String docId, boolean decode) {
        Document d = new Document(docId);
        try (Jedis conn = _conn()) {
            List<Object> res = sendCommand(conn, commands.getGetCommand(), indexName, docId).getObjectMultiBulkReply();
            if (res == null) {
                return null;
            }
            handleListMapping(res, d::set, decode);
            return d;
        }
    }

    /**
     * Drop the index and all associated keys, including documents
     *
     * @return true on success
     */
    public boolean dropIndex() {
        return dropIndex(false);
    }

    /**
     * Drop the index and associated keys, including documents
     *
     * @param missingOk If the index does not exist, don't throw an exception, but return false instead
     * @return True if the index was dropped, false if it did not exist (or some other error occurred).
     */
    public boolean dropIndex(boolean missingOk) {
        String r;
        try (Jedis conn = _conn()) {
            r = sendCommand(conn, commands.getDropCommand(), this.indexName).getStatusCodeReply();
        } catch (JedisDataException ex) {
            if (missingOk && ex.getMessage().toLowerCase().contains("unknown")) {
                return false;
            } else {
                throw ex;
            }
        }
        return r.equals("OK");
    }

    @Override
    public Long addSuggestion(Suggestion suggestion, boolean increment) {
        List<String> args = new ArrayList<>(
                Arrays.asList(this.indexName, suggestion.getString(), Double.toString(suggestion.getScore())));

        if (increment) {
            args.add(INCREMENT_FLAG);
        }
        if (suggestion.getPayload() != null) {
            args.add(PAYLOAD_FLAG);
            args.add(suggestion.getPayload());
        }

        try (Jedis conn = _conn()) {
            return sendCommand(conn, AutoCompleter.Command.SUGADD, args.toArray(new String[args.size()])).getIntegerReply();
        }
    }

    @Override
    public List<Suggestion> getSuggestion(String prefix, SuggestionOptions suggestionOptions) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(this.indexName, prefix, MAX_FLAG, Integer.toString(suggestionOptions.getMax())));

        if (suggestionOptions.isFuzzy()) {
            args.add(FUZZY_FLAG);
        }
                
        Optional<With> options = suggestionOptions.getWith();
        if (!options.isPresent()) {
        	return getSuggestions(args);
        }
        
        With with = options.get();
        args.addAll(Arrays.asList(with.getFlags()));
        switch (with) {
            case PAYLOAD_AND_SCORES:
                return getSuggestionsWithPayloadAndScores(args);
            case PAYLOAD: 
                return getSuggestionsWithPayload(args);
            default: 
                return getSuggestionsWithScores(args);
        }
    }

    private List<Suggestion> getSuggestions(List<String> args) {
        final List<Suggestion> list = new ArrayList<>();
        try (Jedis conn = _conn()) {
            final List<String> result = sendCommand(conn, AutoCompleter.Command.SUGGET, args.toArray(new String[args.size()])).getMultiBulkReply();
            result.forEach(str -> list.add(Suggestion.builder().str(str).build()));
        }
        return list;
    }

    private List<Suggestion> getSuggestionsWithScores(List<String> args) {
        final List<Suggestion> list = new ArrayList<>();
        try (Jedis conn = _conn()) {
            final List<String> result = sendCommand(conn, AutoCompleter.Command.SUGGET, args.toArray(new String[args.size()])).getMultiBulkReply();
            for (int i = 1; i < result.size() + 1; i++) {
                if (i % 2 == 0) {
                    Suggestion.Builder builder = Suggestion.builder();
                    builder.str(result.get(i - 2));
                    builder.score(Double.parseDouble(result.get(i - 1)));
                    list.add(builder.build());
                }
            }
        }
        return list;
    }

    private List<Suggestion> getSuggestionsWithPayload(List<String> args) {
        final List<Suggestion> list = new ArrayList<>();
        try (Jedis conn = _conn()) {
            final List<String> result = sendCommand(conn, AutoCompleter.Command.SUGGET, args.toArray(new String[args.size()])).getMultiBulkReply();
            for (int i = 1; i < result.size() + 1; i++) {
                if (i % 2 == 0) {
                    Suggestion.Builder builder = Suggestion.builder();
                    builder.str(result.get(i - 2));
                    builder.payload(result.get(i - 1));
                    list.add(builder.build());
                }
            }
        }
        return list;
    }

    private List<Suggestion> getSuggestionsWithPayloadAndScores(List<String> args) {
        final List<Suggestion> list = new ArrayList<>();
        try (Jedis conn = _conn()) {
            final List<String> result = sendCommand(conn, AutoCompleter.Command.SUGGET, args.toArray(new String[args.size()])).getMultiBulkReply();
            for (int i = 1; i < result.size() + 1; i++) {
                if (i % 3 == 0) {
                    Suggestion.Builder builder = Suggestion.builder();
                    builder.str(result.get(i - 3));
                    builder.score(Double.parseDouble(result.get(i - 2)));
                    builder.payload(result.get(i - 1));
                    list.add(builder.build());
                }
            }
        }
        return list;
    }

    @FunctionalInterface
    private interface KVHandler {
        void apply(String key, Object value);
    }

    /**
     * IndexOptions encapsulates flags for index creation and shuold be given to the client on index creation
     */
    public static class IndexOptions {
        /**
         * Set this to tell the index not to save term offset vectors. This reduces memory consumption but does not
         * allow performing exact matches, and reduces overall relevance of multi-term queries
         */
        public static final int USE_TERM_OFFSETS = 0x01;

        /**
         * If set (default), we keep flags per index record telling us what fields the term appeared on,
         * and allowing us to filter results by field
         */
        public static final int KEEP_FIELD_FLAGS = 0x02;

        /**
         * With each document:term record, store how often the term appears within the document. This can be used
         * for sorting documents by their relevance to the given term.
         */
        public static final int KEEP_TERM_FREQUENCIES = 0x08;

        public static final int DEFAULT_FLAGS = USE_TERM_OFFSETS | KEEP_FIELD_FLAGS | KEEP_TERM_FREQUENCIES;

        private final int flags;
        private List<String> stopwords;
        private long expire = 0L;

        /**
         * Default constructor
         *
         * @param flags flag mask
         */
        public IndexOptions(int flags) {
            this.flags = flags;
        }

        /**
         * The default indexing options - use term offsets and keep fields flags
         */
        public static IndexOptions defaultOptions() {
            return new IndexOptions(DEFAULT_FLAGS);
        }
        
        /**
         * The default indexing options - use term offsets and keep fields flags
         * @deprecated use {@link #defaultOptions()} instead
         */
        @Deprecated
        public static IndexOptions Default() {
            return IndexOptions.defaultOptions();
        }


        /**
         * Set a custom stopword list
         *
         * @param stopwords the list of stopwords
         * @return the options object itself, for builder-style construction
         */
        public IndexOptions setStopwords(String... stopwords) {
            this.stopwords = Arrays.asList(stopwords);
            return this;
        }
        
        /**
         * Set a custom stopword list
         *
         * @param stopwords the list of stopwords
         * @return the options object itself, for builder-style construction
         * @deprecated use {@link #setStopwords(String...)} instead
         */
        @Deprecated
        public IndexOptions SetStopwords(String... stopwords) {
            return this.setStopwords(stopwords);
        }

        /**
         * Set the index to contain no stopwords, overriding the default list
         *
         * @return the options object itself, for builder-style constructions
         */
        public IndexOptions setNoStopwords() {
          stopwords = new ArrayList<>(0);
          return this;          
        }
        
        /**
         * Set the index to contain no stopwords, overriding the default list
         *
         * @return the options object itself, for builder-style constructions
         * @deprecated Use {@link #setNoStopwords()} instead 
         */
        @Deprecated
        public IndexOptions SetNoStopwords() {
            return this.setNoStopwords();
        }
        
        /**
         * Temporary
         * @param expire
         * @return
         */
        public IndexOptions setTemporary(long expire) {
          this.expire = expire;
          return this;
        }
        

        public void serializeRedisArgs(List<String> args) {

            if ((flags & USE_TERM_OFFSETS) == 0) {
                args.add("NOOFFSETS");
            }
            if ((flags & KEEP_FIELD_FLAGS) == 0) {
                args.add("NOFIELDS");
            }
            if ((flags & KEEP_TERM_FREQUENCIES) == 0) {
                args.add("NOFREQS");
            }
            if(expire > 0) {
              args.add("TEMPORARY");
              args.add(Long.toString(this.expire));
            }

            if (stopwords != null) {
                args.add("STOPWORDS");
                args.add(Integer.toString(stopwords.size()));
                if (!stopwords.isEmpty()) {
                    args.addAll(stopwords);
                }

            }
        }
    }

}
