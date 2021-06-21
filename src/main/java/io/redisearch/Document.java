package io.redisearch;

import com.google.gson.Gson;

import redis.clients.jedis.util.SafeEncoder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Document represents a single indexed document or entity in the engine
 */
public class Document implements Serializable {

    private static final Gson gson = new Gson();
  
    private String id;
    private double score;
    private byte[] payload;
    private Map<String, Object> properties;

    @Deprecated
    public Document(String id, Double score) {
        this(id, new HashMap<>(), (double) score);
    }

    public Document(String id, double score) {
        this(id, new HashMap<>(), score);
    }

    public Document(String id) {
        this(id, 1.0);
    }

    public Document(String id, Map<String,Object> fields) {
        this(id, fields, 1.0f);
    }

    public Document(String id, Map<String, Object> fields, double score) {
        this(id, fields, score, null);
    }

    public Document(String id, Map<String, Object> fields, double score, byte[] payload) {
        this.id = id;
        this.properties = new HashMap<>(fields);
        this.score = score;
        this.payload = payload;
    }

    public Iterable<Map.Entry<String, Object>> getProperties() {
        return properties.entrySet();
    }

    @Deprecated
    public static Document load(String id, Double score, byte[] payload, List<byte[]> fields) {
        return Document.load(id, (double) score, payload, fields);
    }

    public static Document load(String id, double score, byte[] payload, List<byte[]> fields) {
        return Document.load(id, score, payload, fields, true);
    }

    @Deprecated
    public static Document load(String id, Double score, byte[] payload, List<byte[]> fields, boolean decode) {
        return load(id, (double) score, payload, fields, decode);
    }

    public static Document load(String id, double score, byte[] payload, List<byte[]> fields, boolean decode) {
        Document ret = new Document(id, score);
        ret.payload = payload;
        if (fields != null) {
            for (int i = 0; i < fields.size(); i += 2) {
                ret.set(SafeEncoder.encode(fields.get(i)), decode ? SafeEncoder.encode(fields.get(i + 1)) : fields.get(i + 1));
            }
        }
        return ret;
    }

    public Document set(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    /**
     * return the property value inside a key
     *
     * @param key key of the property
     * 
     * @return the property value 
     */
    public Object get(String key) {
        return properties.get(key);
    }

    /**
     * return the property value inside a key
     *
     * @param key key of the property
     * 
     * @return the property value 
     */
    public String getString(String key) {
        Object value = properties.get(key);
        if(value instanceof String) {
          return (String)value;
        }
        return value instanceof byte[] ? SafeEncoder.encode((byte[])value) : value.toString();
    }

    /**
     * @return the document's score
     */
    public double getScore() {
        return score;
    }

    public byte[] getPayload() {
        return payload;
    }

    /**
     * Set the document's score
     *
     * @param score new score to set
     * @return the document itself
     */
    public Document setScore(float score) {
        this.score = score;
        return this;
    }

    /**
     * @return the document's id
     */
    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }
}
