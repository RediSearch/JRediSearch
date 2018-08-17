package io.redisearch.api;

import io.redisearch.Document;
import io.redisearch.client.AddOptions;

import java.util.Map;

public interface DocumentClient {

    boolean addDocument(Document doc, AddOptions options);

    boolean replaceDocument(String docId, double score, Map<String, Object> fields);

    boolean updateDocument(String docId, double score, Map<String, Object> fields);

    boolean deleteDocument(String docId);

    Document getDocument(String docId);

    boolean addHash(String docId, double score, boolean replace);

}
