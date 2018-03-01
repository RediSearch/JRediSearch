package io.redisearch.client;

import io.redisearch.Document;
import io.redisearch.AggregationResult;
import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import io.redisearch.aggregation.AggregationRequest;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;
import java.util.function.Function;

/**
 * Client is the main RediSearch client class, wrapping connection management and all RediSearch commands
 */
public class Client {

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
         * for sorting documents by their relevancy to the given term.
         */
        public static final int KEEP_TERM_FREQUENCIES = 0x08;

        public static final int DEFAULT_FLAGS = USE_TERM_OFFSETS | KEEP_FIELD_FLAGS | KEEP_TERM_FREQUENCIES;

        int flags = 0x0;

        List<String> stopwords = null;

        /**
         * Default constructor
         * @param flags flag mask
         */
        public IndexOptions(int flags) {
            this.flags = flags;
            stopwords = null;
        }

        /**
         * Set a custom stopword list
         * @return the options object itself, for builder-style construction
         */
        public IndexOptions SetStopwords(String ...stopwords) {
            this.stopwords = Arrays.asList(stopwords);
            return this;
        }

        /**
         * Set the index to contain no stopwords, overriding the default list
         * @return the options object itself, for builder-style constructions
         */
        public IndexOptions SetNoStopwords() {
            stopwords = new ArrayList<>(0);
            return this;
        }


        /**
         * The default indexing options - use term offsets and keep fields flags
         */
        public static IndexOptions Default() {
            return new IndexOptions(DEFAULT_FLAGS);
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

            if (stopwords!=null) {

                args.add("STOPWORDS");
                args.add(String.format("%d", stopwords.size()));
                if (stopwords.size() > 0) {
                    args.addAll(stopwords);
                }

            }
        }
    }

    ;

    private final String indexName;
    private JedisPool pool;

    Jedis _conn() {
        return pool.getResource();
    }
    protected Commands.CommandProvider commands;

    /**
     * Create a new client to a RediSearch index
     * @param indexName the name of the index we are connecting to or creating
     * @param host the redis host
     * @param port the redis pot
     */
    public Client(String indexName, String host, int port, int timeout, int poolSize) {
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

        pool = new JedisPool(conf, host, port, timeout);

        this.indexName = indexName;
        this.commands = new Commands.SingleNodeCommands();
    }

    public Client(String indexName, String host, int port) {
        this(indexName, host, port, 500, 100);
    }

    /**
     * Create the index definition in redis
     * @param schema a schema definition, see {@link Schema}
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
            String rep = conn.getClient()
                    .sendCommand(commands.getCreateCommand(), args.toArray(new String[args.size()]))
                    .getStatusCodeReply();
            return rep.equals("OK");
        }
    }

    /**
     * Search the index
     * @param q a {@link Query} object with the query string and optional parameters
     * @return a {@link SearchResult} object with the results
     */
    public SearchResult search(Query q) {
        ArrayList<byte[]> args = new ArrayList(4);
        args.add(indexName.getBytes());
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            List<Object> resp = conn.getClient().
                    sendCommand(commands.getSearchCommand(),
                            args.toArray(new byte[args.size()][])).getObjectMultiBulkReply();
            return new SearchResult(resp, !q.getNoContent(), q.getWithScores(), q.getWithPayloads());
        }
    }

    public AggregationResult aggregate(AggregationRequest q) {
        ArrayList<byte[]> args = new ArrayList<>();
        args.add(indexName.getBytes());
        q.serializeRedisArgs(args);

        Jedis conn = _conn();
        List<Object> resp = conn.getClient().sendCommand(commands.getSearchCommand(), args.toArray(new byte[args.size()][])).getObjectMultiBulkReply();
        conn.close();
        return new AggregationResult(resp);
    }

    /**
     * Generate an explanatory textual query tree for this query string
     * @param q The query to explain
     * @return A string describing this query
     */
    public String explain(Query q) {
        ArrayList<byte[]> args = new ArrayList(4);
        args.add(indexName.getBytes());
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            return conn.getClient().
                    sendCommand(commands.getExplainCommand(), args.toArray(new byte[args.size()][])).getStatusCodeReply();
        }
    }

    /**
     * Add a single document to the query
     * @param docId the id of the document. It cannot belong to a document already in the index unless replace is set
     * @param score the document's score, floating point number between 0 and 1
     * @param fields a map of the document's fields
     * @param noSave if set, we only index the document and do not save its contents. This allows fetching just doc ids
     * @param replace if set, and the document already exists, we reindex and update it
     * @param payload if set, we can save a payload in the index to be retrieved or evaluated by scoring functions on the server
     * @return
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
     * @param doc The document to add
     * @param options Options for the operation
     * @return true if the operation succeeded, false otherwise. Note that if the operation fails, an exception
     *  will be thrown
     */
    public boolean addDocument(Document doc, AddOptions options) {
        ArrayList<byte[]> args = new ArrayList<>(
                Arrays.asList(indexName.getBytes(), doc.getId().getBytes(), Double.toString(doc.getScore()).getBytes()));
        if (options.getNosave()) {
            args.add("NOSAVE".getBytes());
        }
        if (options.getReplacementPolicy() != AddOptions.ReplacementPolicy.NONE) {
            args.add("REPLACE".getBytes());
            if (options.getReplacementPolicy() == AddOptions.ReplacementPolicy.PARTIAL) {
                args.add("PARTIAL".getBytes());
            }
        }
        if (options.getLanguage() != null && !options.getLanguage().isEmpty()) {
            args.add("LANGUAGE".getBytes());
            args.add(options.getLanguage().getBytes());
        }
        if (doc.getPayload() != null) {
            args.add("PAYLOAD".getBytes());
            // TODO: Fix this
            args.add(doc.getPayload());
        }

        args.add("FIELDS".getBytes());
        for (Map.Entry<String, Object> ent : doc.getProperties()) {
            args.add(ent.getKey().getBytes());
            args.add(ent.getValue().toString().getBytes());
        }

        try (Jedis conn = _conn()) {
            String resp = conn.getClient().sendCommand(commands.getAddCommand(),
                    args.toArray(new byte[args.size()][]))
                    .getStatusCodeReply();
            return resp.equals("OK");
        }
    }

    /**
     * replaceDocument is a convenience for calling addDocument with replace=true
     */
    public boolean replaceDocument(String docId, double score, Map<String, Object> fields ) {
        return addDocument( docId, score, fields,false, true, null);
    }

    /**
     * Replace specific fields in a document. Unlike #replaceDocument(), fields not present in the field list
     * are not erased, but retained. This avoids reindexing the entire document if the new values are not
     * indexed (though a reindex will happen
     * @param docId
     * @param score
     * @param fields
     * @return
     */
    public boolean updateDocument(String docId, double score, Map<String, Object> fields) {
        return doAddDocument(docId, score, fields, false, true, true, null);
    }

    /** See above */
    public boolean addDocument(String docId, double score, Map<String, Object> fields) {
        return this.addDocument(docId, score, fields, false, false, null);
    }
    /** See above */
    public boolean addDocument(String docId, Map<String, Object> fields) {
        return this.addDocument(docId, 1, fields, false, false, null);
    }

    /** Index a document already in redis as a HASH key.
     *
     * @param docId the id of the document in redis. This must match an existing, unindexed HASH key
     * @param score the document's index score, between 0 and 1
     * @param replace if set, and the document already exists, we reindex and update it
     * @return true on success
     */
    public boolean addHash(String docId, double score, boolean replace) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName, docId, Double.toString(score)));

        if (replace) {
            args.add("REPLACE");
        }

        try (Jedis conn = _conn()) {
            String resp = conn.getClient().sendCommand(commands.getAddHashCommand(),
                    args.toArray(new String[args.size()])).getStatusCodeReply();
            return resp.equals("OK");
        }
    }

    @FunctionalInterface
    private interface KVHandler {
        void apply(String key, Object value);
    }

    private static void handleListMapping(List<Object> items, KVHandler handler) {
        for (int i = 0; i < items.size(); i += 2) {
            String key = new String((byte[])items.get(i));
            Object val = items.get(i+1);
            if (val.getClass().equals((new byte[]{}).getClass())) {
                val = new String((byte[]) val);
            }
            handler.apply(key, val);
        }
    }

    /** Get the index info, including memory consumption and other statistics.
     * TODO: Make a class for easier access to the index properties
     * @return a map of key/value pairs
     */
    public Map<String, Object> getInfo() {
        List<Object> res;
        try (Jedis conn = _conn()) {
            res = conn.getClient().sendCommand(commands.getInfoCommand(), this.indexName).getObjectMultiBulkReply();
        }

        Map<String, Object> info = new HashMap<>();
        handleListMapping(res, info::put);
        return info;
    }

    /**
     * Delete a document from the index.
     * @param docId the document's id
     * @return true if it has been deleted, false if it did not exist
     */
    public boolean deleteDocument(String docId) {
        try (Jedis conn = _conn()) {
            Long r = conn.getClient().sendCommand(commands.getDelCommand(), this.indexName, docId).getIntegerReply();
            return r == 1;
        }
    }

    /**
     * Get a document from the index
     * @param docId The document ID to retrieve
     * @return The document as stored in the index. If the document does not exist, null is returned.
     */
    public Document getDocument(String docId) {
        Document d = new Document(docId);
        try (Jedis conn = _conn()) {
            List<Object> res = conn.getClient().sendCommand(commands.getGetCommand(), indexName, docId).getObjectMultiBulkReply();
            if (res == null) {
                return null;
            }
            handleListMapping(res, d::set);
            return d;
        }
    }

    /**
     * Drop the index and all associated keys, including documents
     * @return true on success
     */
    public boolean dropIndex() {
        return dropIndex(false);
    }

    /**
     * Drop the index and associated keys, including documents
     * @param missingOk If the index does not exist, don't throw an exception, but return false instead
     * @return True if the index was dropped, false if it did not exist (or some other error occurred).
     */
    public boolean dropIndex(boolean missingOk) {
        String r;
        try (Jedis conn = _conn()) {
            r = conn.getClient().sendCommand(commands.getDropCommand(), this.indexName).getStatusCodeReply();
        } catch (JedisDataException ex) {
            if (missingOk && ex.getMessage().toLowerCase().contains("unknown")) {
                return false;
            } else {
                throw ex;
            }
        }
        return r.equals("OK");
    }
}
