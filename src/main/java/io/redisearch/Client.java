package io.redisearch;

import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;

import java.util.List;
import java.util.Map;

public interface Client {

    String INCREMENT_FLAG = "INCR";
    String PAYLOAD_FLAG = "PAYLOAD";
    String WITHPAYLOADS_FLAG = "WITHPAYLOADS";
    String MAX_FLAG = "MAX";
    String FUZZY_FLAG = "FUZZY";
    String WITHSCORES_FLAG = "WITHSCORES";

    boolean createIndex(Schema schema, io.redisearch.client.Client.IndexOptions options);

    SearchResult search(Query q);

    AggregationResult aggregate(AggregationRequest q);

    String explain(Query q);

    boolean addDocument(String docId, double score, Map<String, Object> fields, boolean noSave, boolean replace, byte[] payload);

    boolean addDocument(Document doc, AddOptions options);

    boolean addDocument(Document doc);

    boolean replaceDocument(String docId, double score, Map<String, Object> fields);

    boolean updateDocument(String docId, double score, Map<String, Object> fields);

    boolean addDocument(String docId, double score, Map<String, Object> fields);

    boolean addDocument(String docId, Map<String, Object> fields);

    boolean addHash(String docId, double score, boolean replace);

    Map<String, Object> getInfo();

    boolean deleteDocument(String docId);

    Document getDocument(String docId);

    boolean dropIndex();

    boolean dropIndex(boolean missingOk);

    /**
     * Add a word to the suggestion index for redis plugin
     *
     * @param suggestion the Suggestion to be added
     * @param increment  if we should increment this suggestion or not
     * @return the current size after your added suggestion has been added
     */
    Long addSuggestion(Suggestion suggestion, boolean increment);

    /**
     * No Score will be retrieved from the Redis search plugin a payload will be returned if it was added
     *
     * @param prefix the start of the word to match on
     * @param max    the total number of results
     * @param fuzzy  should it be fuzzy or not
     * @return the list of matches or an empty list
     */
    List<Suggestion> getSuggestionWithPayload(String prefix, int max, boolean fuzzy);

    /**
     * No Payload or Score will be retrieved from the Redis search plugin
     *
     * @param prefix the start of the word to match on
     * @param max    the total number of results
     * @param fuzzy  should it be fuzzy or not
     * @return the list of matches or an empty list
     */
    List<Suggestion> getSuggestion(String prefix, int max, boolean fuzzy);

    /**
     * No Payload will be retrieved from the Redis search plugin score returned as added when loading suggestion
     *
     * @param prefix the start of the word to match on
     * @param max    the total number of results
     * @param fuzzy  should it be fuzzy or not
     * @return the list of matches or an empty list
     */
    List<Suggestion> getSuggestionWithScore(String prefix, int max, boolean fuzzy);

    /**
     * Will ask for both Score and Payload to added to the return, still depends on if it was added when index for
     * suggestion was given such data
     *
     * @param prefix the start of the word to match on
     * @param max    the total number of results
     * @param fuzzy  should it be fuzzy or not
     * @return the list of matches or an empty list
     */
    List<Suggestion> getSuggestionWithScoreAndPayload(String prefix, int max, boolean fuzzy);


}
