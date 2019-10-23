package io.redisearch;

import io.redisearch.Schema.Field;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;
import io.redisearch.client.SuggestionOptions;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface Client extends Closeable{
  
    /**
     * @Deprecated use {@link Keywords#INCR} instead 
     */
    @Deprecated
    String INCREMENT_FLAG = "INCR";
    
    /**
     * @Deprecated use {@link Keywords#PAYLOAD} instead 
     */
    @Deprecated
    String PAYLOAD_FLAG = "PAYLOAD";
    
    /**
     * @Deprecated use {@link Keywords#MAX} instead 
     */
    @Deprecated
    String MAX_FLAG = "MAX";
    
    /**
     * @Deprecated use {@link Keywords#FUZZY} instead 
     */
    @Deprecated
    String FUZZY_FLAG = "FUZZY";
    
    /**
     * @Deprecated use {@link Keywords#DD} instead 
     */
    @Deprecated
    String DELETE_DOCUMENT = "DD";

    boolean createIndex(Schema schema, io.redisearch.client.Client.IndexOptions options);

    SearchResult search(Query q);

    /**
     * @deprecated use {@link #aggregate(AggregationBuilder)} instead
     */
    @Deprecated
    AggregationResult aggregate(AggregationRequest q);
    
    AggregationResult aggregate(AggregationBuilder q);
    
    boolean cursorDelete(long cursorId);
    
    AggregationResult cursorRead(long cursorId, int count);

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

    boolean alterIndex(Field... fields);

    SearchResult search(Query q, boolean decode);

    boolean addDocument(String docId, double score, Map<String, Object> fields, boolean noSave, boolean replace, byte[] payload);

    boolean addDocument(Document doc);

    boolean[] addDocuments(Document... docs);

    boolean[] addDocuments(AddOptions options, Document... docs);

    boolean addDocument(String docId, double score, Map<String, Object> fields);

    boolean addDocument(String docId, Map<String, Object> fields);

    boolean[] deleteDocuments(boolean deleteDocuments, String... docIds);

    boolean deleteDocument(String docId, boolean deleteDocument);

    Document getDocument(String docId, boolean decode);

    List<Document> getDocuments(String... docIds);

    List<Document> getDocuments(boolean decode, String... docIds);

    boolean dropIndex();
}
