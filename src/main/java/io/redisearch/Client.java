package io.redisearch;

import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;
import io.redisearch.client.SuggestionOptions;

import java.util.List;
import java.util.Map;

public interface Client {

    String INCREMENT_FLAG = "INCR";
    String PAYLOAD_FLAG = "PAYLOAD";
    String MAX_FLAG = "MAX";
    String FUZZY_FLAG = "FUZZY";

    boolean createIndex(Schema schema, io.redisearch.client.Client.IndexOptions options);

    SearchResult search(Query q);

    AggregationResult aggregate(AggregationRequest q);

    String explain(Query q);

    boolean addDocument(Document doc, AddOptions options);

    boolean replaceDocument(String docId, double score, Map<String, Object> fields);

    boolean updateDocument(String docId, double score, Map<String, Object> fields);

    boolean addHash(String docId, double score, boolean replace);

    Map<String, Object> getInfo();

    boolean deleteDocument(String docId);

    Document getDocument(String docId);

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
     * Request the Suggestions based on the prefix
     * @param prefix the partial word
     * @param suggestionOptions the options on what you need returned and other usage
     * @return list of suggestions
     */
    List<Suggestion> getSuggestion(String prefix, SuggestionOptions suggestionOptions);
}
