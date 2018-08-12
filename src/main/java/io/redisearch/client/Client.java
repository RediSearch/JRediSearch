package io.redisearch.client;

import io.redisearch.*;
import io.redisearch.aggregation.AggregationRequest;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

/**
 * Client is the main RediSearch client class, wrapping connection management and all RediSearch commands
 */
class Client implements io.redisearch.Client {

    private final String indexName;
    protected Commands.CommandProvider commands;
    private JedisPool pool;

    protected Client(String indexName, String host, int port, int timeout, int poolSize, String password) {
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

        pool = new JedisPool(conf, host, port, timeout, password);

        this.indexName = indexName;
        this.commands = new Commands.SingleNodeCommands();
    }

    private static void handleListMapping(List<Object> items, KVHandler handler) {
        for (int i = 0; i < items.size(); i += 2) {
            String key = new String((byte[]) items.get(i));
            Object val = items.get(i + 1);
            if (val.getClass().equals((new byte[]{}).getClass())) {
                val = new String((byte[]) val);
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
     * Create the index definition in redis
     *
     * @param schema  a schema definition, see {@link Schema}
     * @param options index option flags, see {@link IndexOptions}
     * @return true if successful
     */
    @Override
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
    @Override
    public SearchResult search(Query q) {
        ArrayList<byte[]> args = new ArrayList(4);
        args.add(indexName.getBytes());
        q.serializeRedisArgs(args);

        try (Jedis conn = _conn()) {
            List<Object> resp =
                    sendCommand(conn, commands.getSearchCommand(),
                            args.toArray(new byte[args.size()][])).getObjectMultiBulkReply();
            return new SearchResult(resp, !q.getNoContent(), q.getWithScores(), q.getWithPayloads());
        }
    }

    @Override
    public AggregationResult aggregate(AggregationRequest q) {
        ArrayList<byte[]> args = new ArrayList<>();
        args.add(indexName.getBytes());
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
    @Override
    public String explain(Query q) {
        ArrayList<byte[]> args = new ArrayList(4);
        args.add(indexName.getBytes());
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
     * @return
     */
    @Override
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
     * @param doc     The document to add
     * @param options Options for the operation
     * @return true if the operation succeeded, false otherwise. Note that if the operation fails, an exception
     * will be thrown
     */
    @Override
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
            String resp = sendCommand(conn, commands.getAddCommand(), args.toArray(new byte[args.size()][]))
                    .getStatusCodeReply();
            return resp.equals("OK");
        }
    }

    @Override
    public boolean addDocument(Document doc) {
        return addDocument(doc, new AddOptions());
    }

    /**
     * replaceDocument is a convenience for calling addDocument with replace=true
     */
    @Override
    public boolean replaceDocument(String docId, double score, Map<String, Object> fields) {
        return addDocument(docId, score, fields, false, true, null);
    }

    /**
     * Replace specific fields in a document. Unlike #replaceDocument(), fields not present in the field list
     * are not erased, but retained. This avoids reindexing the entire document if the new values are not
     * indexed (though a reindex will happen
     *
     * @param docId
     * @param score
     * @param fields
     * @return
     */
    @Override
    public boolean updateDocument(String docId, double score, Map<String, Object> fields) {
        return doAddDocument(docId, score, fields, false, true, true, null);
    }

    /**
     * See above
     */
    @Override
    public boolean addDocument(String docId, double score, Map<String, Object> fields) {
        return this.addDocument(docId, score, fields, false, false, null);
    }

    /**
     * See above
     */
    @Override
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
    @Override
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
    @Override
    public Map<String, Object> getInfo() {
        List<Object> res;
        try (Jedis conn = _conn()) {
            res = sendCommand(conn, commands.getInfoCommand(), this.indexName).getObjectMultiBulkReply();
        }

        Map<String, Object> info = new HashMap<>();
        handleListMapping(res, info::put);
        return info;
    }

    /**
     * Delete a document from the index.
     *
     * @param docId the document's id
     * @return true if it has been deleted, false if it did not exist
     */
    @Override
    public boolean deleteDocument(String docId) {
        try (Jedis conn = _conn()) {
            Long r = sendCommand(conn, commands.getDelCommand(), this.indexName, docId).getIntegerReply();
            return r == 1;
        }
    }

    /**
     * Get a document from the index
     *
     * @param docId The document ID to retrieve
     * @return The document as stored in the index. If the document does not exist, null is returned.
     */
    @Override
    public Document getDocument(String docId) {
        Document d = new Document(docId);
        try (Jedis conn = _conn()) {
            List<Object> res = sendCommand(conn, commands.getGetCommand(), indexName, docId).getObjectMultiBulkReply();
            if (res == null) {
                return null;
            }
            handleListMapping(res, d::set);
            return d;
        }
    }

    /**
     * Drop the index and all associated keys, including documents
     *
     * @return true on success
     */
    @Override
    public boolean dropIndex() {
        return dropIndex(false);
    }

    /**
     * Drop the index and associated keys, including documents
     *
     * @param missingOk If the index does not exist, don't throw an exception, but return false instead
     * @return True if the index was dropped, false if it did not exist (or some other error occurred).
     */
    @Override
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
        ArrayList<byte[]> args = new ArrayList<>(
                Arrays.asList(indexName.getBytes(), suggestion.getString().getBytes(), Double.toString(suggestion.getScore()).getBytes()));

        if (increment) {
            args.add(Commands.GeneralKey.INCREMENT.getRaw());
        }
        if (suggestion.getPayload() != null && suggestion.getPayload().length > 0) {
            args.add(Commands.GeneralKey.PAYLOAD.getRaw());
            args.add(suggestion.getPayload());
        }

        try (Jedis conn = _conn()) {
            return sendCommand(conn, AutoCompleter.Command.SUGADD, args.toArray(new byte[args.size()][])).getIntegerReply();
        }
    }

    public List<String> getSuggestion(String prefix, boolean withPayloads, int max, boolean fuzzy, boolean scores) {
        ArrayList<byte[]> args = new ArrayList<>(
                Arrays.asList(indexName.getBytes(), prefix.getBytes(), Integer.toString(max).getBytes()));

        if (max >= 0) {
            args.add(Commands.GeneralKey.MAX.getRaw());
            args.add(Integer.toString(max).getBytes());
        }
        if (withPayloads) {
            args.add(Commands.GeneralKey.WITHPAYLOADS.getRaw());

        }
        if (fuzzy) {
            args.add(Commands.GeneralKey.FUZZY.getRaw());
        }
        if(scores) {
            args.add(Commands.GeneralKey.SCORES.getRaw());
        }

        try (Jedis conn = _conn()) {
            return sendCommand(conn, AutoCompleter.Command.SUGGET, args.toArray(new byte[args.size()][])).getMultiBulkReply();
        }

    }


    @FunctionalInterface
    private interface KVHandler {
        void apply(String key, Object value);
    }


}
