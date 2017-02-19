package io.redisearch.client;

import io.redisearch.Query;
import io.redisearch.Schema;
import io.redisearch.SearchResult;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by dvirsky on 08/02/17.
 */
public class Client {

    public static class IndexOptions {
        public static final int USE_TERM_OFFSETS = 0x01;
        public static final int KEEP_FIELD_FLAGS = 0x02;
        public static final int USE_SCORE_INDEXES = 0x04;

        int flags = 0x0;

        public IndexOptions(int flags) {
            this.flags = flags;
        }

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

    private Jedis _conn() {
        return pool.getResource();
    }

    public Client(String indexName, String host, int port) {
        pool = new JedisPool(host, port);

        this.indexName = indexName;
    }

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
                .sendCommand(Commands.Command.CREATE, args.toArray(new String[args.size()]))
                .getStatusCodeReply();

        return rep.equals("OK");

    }

    public SearchResult search(Query q) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName));
        q.serializeRedisArgs(args);

        Jedis conn = _conn();
        List<Object> resp = conn.getClient().sendCommand(Commands.Command.SEARCH, args.toArray(new String[args.size()])).getObjectMultiBulkReply();

        return new SearchResult(resp, !q.getNoContent(), q.getWithScores(), q.getWithPayloads());
    }

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
            args.add(payload.toString());
        }

        args.add("FIELDS");
        for (Map.Entry<String, Object> ent : fields.entrySet()) {
            args.add(ent.getKey());
            args.add(ent.getValue().toString());
        }

        Jedis conn = _conn();
        String resp = conn.getClient().sendCommand(Commands.Command.ADD, args.toArray(new String[args.size()])).getStatusCodeReply();
        return resp.equals("OK");

    }

    public boolean addDocument(String docId, double score, Map<String, Object> fields) {
        return this.addDocument(docId, score, fields, false, false, null);
    }

    public boolean addDocument(String docId, Map<String, Object> fields) {
        return this.addDocument(docId, 1, fields, false, false, null);
    }

    /* FT.ADDHASH {index} {docId} {score} [LANGUAGE language] [REPLACE] */

    public boolean addHash(String docId, double score, boolean replace) {
        ArrayList<String> args = new ArrayList<>(Arrays.asList(indexName, docId, Double.toString(score)));

        if (replace) {
            args.add("REPLACE");
        }

        Jedis conn = _conn();
        String resp = conn.getClient().sendCommand(Commands.Command.ADDHASH,
                args.toArray(new String[args.size()])).getStatusCodeReply();
        return resp.equals("OK");
    }

    public static class IndexInfo {

    }

    public IndexInfo getInfo() {
        return null;
    }

    public boolean deleteDocument(String docId) {

        Jedis conn = _conn();
        Long r = conn.getClient().sendCommand(Commands.Command.DEL, this.indexName, docId).getIntegerReply();
        return r == 1;
    }

    public boolean dropIndex() {
        Jedis conn = _conn();
        String r = conn.getClient().sendCommand(Commands.Command.DROP, this.indexName).getStatusCodeReply();
        return r.equals("OK");
    }

    public long optimizeIndex() {
        Jedis conn = _conn();
        return conn.getClient().sendCommand(Commands.Command.DROP, this.indexName).getIntegerReply();
    }


}
