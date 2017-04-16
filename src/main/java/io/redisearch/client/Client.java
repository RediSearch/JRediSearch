package io.redisearch.client;

import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

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
         * If set, we keep an index of the top entries per term, allowing extremely fast single word queries
         * regardless of index size, at the cost of more memory
         */
        public static final int USE_SCORE_INDEXES = 0x04;

        int flags = 0x0;

        public IndexOptions(int flags) {
            this.flags = flags;
        }

        /**
         * The default indexing options - use term offsets and keep fields flags
         */
        public static IndexOptions Default() {
            return new IndexOptions(USE_TERM_OFFSETS | KEEP_FIELD_FLAGS);
        }

        public void serializeRedisArgs(List<String> args) {

            if ((flags & USE_TERM_OFFSETS) == 0) {
                args.add("NOOFFSETS");
            }
            if ((flags & KEEP_FIELD_FLAGS) == 0) {
                args.add("NOFIELDS");
            }
            if ((flags & USE_SCORE_INDEXES) == 0) {
                args.add("NOSCOREIDX");
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
    public Client(String indexName, String host, int port) {
        pool = new JedisPool(host, port);

        this.indexName = indexName;
        this.commands = new Commands.SingleNodeCommands();
    }


    /**
     * Create the index definition in redis
     * @param schema a schema definition, see {@link Schema}
     * @param options index option flags, see {@link IndexOptions}
     * @return true if successful
     */
    public boolean createIndex(Schema schema, IndexOptions options) {
        Jedis conn = _conn();

        ArrayList<String> args = new ArrayList<>();

        args.add(indexName);

        options.serializeRedisArgs(args);

        args.add("SCHEMA");

        for (Schema.Field f : schema.fields) {
            f.serializeRedisArgs(args);
        }

        String rep = conn.getClient()
                .sendCommand(commands.getCreateCommand(),
                             args.toArray(new String[args.size()]))
                .getStatusCodeReply();
        conn.close();
        return rep.equals("OK");

    }

    /**
     * Search the index
     * @param q a {@link Query} object with the query string and optional parameters
     * @return a {@link SearchResult} object with the results
     */
    public SearchResult search(Query q) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName));
        q.serializeRedisArgs(args);

        Jedis conn = _conn();
        List<Object> resp = conn.getClient().sendCommand(commands.getSearchCommand(), args.toArray(new String[args.size()])).getObjectMultiBulkReply();
        conn.close();
        return new SearchResult(resp, !q.getNoContent(), q.getWithScores(), q.getWithPayloads());
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
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName, docId, Double.toString(score)));
        if (noSave) {
            args.add("NOSAVE");
        }
        if (replace) {
            args.add("REPLACE");
        }
        if (payload != null) {
            args.add("PAYLOAD");
            // TODO: Fix this
            args.add(new String(payload));
        }

        args.add("FIELDS");
        for (Map.Entry<String, Object> ent : fields.entrySet()) {
            args.add(ent.getKey());
            args.add(ent.getValue().toString());
        }

        Jedis conn = _conn();

        String resp = conn.getClient().sendCommand(commands.getAddCommand(),
                args.toArray(new String[args.size()]))
                .getStatusCodeReply();
        conn.close();
        return resp.equals("OK");

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

        Jedis conn = _conn();
        String resp = conn.getClient().sendCommand(commands.getAddHashCommand(),
                args.toArray(new String[args.size()])).getStatusCodeReply();
        conn.close();
        return resp.equals("OK");
    }

    /** Get the index info, including memory consumption and other statistics.
     * TODO: Make a class for easier access to the index properties
     * @return a map of key/value pairs
     */
    public Map<String, Object> getInfo() {

        Jedis conn = _conn();
        List<Object> res = conn.getClient().sendCommand(commands.getInfoCommand(), this.indexName).getObjectMultiBulkReply();
        conn.close();
        Map<String, Object> info = new HashMap<>();
        for (int i = 0; i < res.size(); i += 2) {
            String key = new String((byte[]) res.get(i));
            Object val = res.get(i + 1);
            if (val.getClass().equals((new byte[]{}).getClass())) {
                val = new String((byte[]) val);
            }
            info.put(key, val);
        }
        return info;
    }

    /**
     * Delete a document from the index.
     * @param docId the document's id
     * @return true if it has been deleted, false if it did not exist
     */
    public boolean deleteDocument(String docId) {

        Jedis conn = _conn();
        Long r = conn.getClient().sendCommand(commands.getDelCommand(), this.indexName, docId).getIntegerReply();
        conn.close();
        return r == 1;
    }

    /**
     * Drop the index and all associated keys, including documents
     * @return true on success
     */
    public boolean dropIndex() {
        Jedis conn = _conn();
        String r = conn.getClient().sendCommand(commands.getDropCommand(), this.indexName).getStatusCodeReply();
        conn.close();
        return r.equals("OK");
    }

    /** Optimize memory consumption of the index by removing extra saved capacity. This does not affect speed */
    public long optimizeIndex() {
        Jedis conn = _conn();
        long ret = conn.getClient().sendCommand(commands.getOptimizeCommand(), this.indexName).getIntegerReply();
        conn.close();
        return ret;
    }


}
