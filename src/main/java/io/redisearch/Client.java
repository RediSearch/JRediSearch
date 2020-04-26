package io.redisearch;

import io.redisearch.Schema.Field;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;
import io.redisearch.client.ConfigOption;
import io.redisearch.client.SuggestionOptions;
import io.redisearch.client.Client.IndexOptions;

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

    /**
     * Create the index definition in redis
     *
     * @param schema  a schema definition, see {@link Schema}
     * @param options index option flags, see {@link IndexOptions}
     * @return true if successful
     */
    boolean createIndex(Schema schema, io.redisearch.client.Client.IndexOptions options);

    /**
     * Search the index
     *
     * @param q a {@link Query} object with the query string and optional parameters
     * @return a {@link SearchResult} object with the results
     */
    SearchResult search(Query q);

    /**
     * Search the index
     *
     * @param q a {@link Query} object with the query string and optional parameters
     * @param decode <code>false</code> - keeps the fields value as byte[] 
     * 
     * @return a {@link SearchResult} object with the results
     */
    SearchResult search(Query q, boolean decode);
    
    /**
     * @deprecated use {@link #aggregate(AggregationBuilder)} instead
     */
    @Deprecated
    AggregationResult aggregate(AggregationRequest q);
    
    /**
     * Search and Aggregate the index
     *
     * @param q a {@link AggregationBuilder} object with the query string and optional aggregation parameters
     * 
     * @return a {@link AggregationResult} object with the results
     */
    AggregationResult aggregate(AggregationBuilder q);
    
    /**
     * Delete a cursor from the index.
     *
     * @param cursorId the cursor's id
     * @return true if it has been deleted, false if it did not exist 
     */    
    boolean cursorDelete(long cursorId);
    
    /**
     * Read from an existing cursor
     *
     * @param cursorId the cursor's id
     * @param count limit the amount of returned results
     * 
     * @return a {@link AggregationResult} object with the results
     */
    AggregationResult cursorRead(long cursorId, int count);

    /**
     * Generate an explanatory textual query tree for this query string
     *
     * @param q The query to explain
     * @return A string describing this query
     */
    String explain(Query q);

    /**
     * Add a document to the index
     *
     * @param doc     The document to add
     * @param options Options for the operation
     * @return true on success
     */
    boolean addDocument(Document doc, AddOptions options);

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
    boolean addDocument(String docId, double score, Map<String, Object> fields, boolean noSave, boolean replace, byte[] payload);

    /**
     * Add a document to the index
     * 
     * @param doc The document to add
     * @return true on success
     */
    boolean addDocument(Document doc);

    /**
     * Add a document to the index
     * 
     * @param docs The document to add
     * @return true on success
     */
    boolean[] addDocuments(Document... docs);

    /**
     * Add a batch of documents to the index
     * @param options Options for the operation
     * @param docs The documents to add
     * @return true on success for each document 
     */
    boolean[] addDocuments(AddOptions options, Document... docs);

    /**
     * See {@link #updateDocument(String, double, Map)}
     */
    boolean addDocument(String docId, double score, Map<String, Object> fields);

    /**
     * See {@link #updateDocument(String, double, Map)}
     */
    boolean addDocument(String docId, Map<String, Object> fields);
    
    /**
     * replaceDocument is a convenience for calling addDocument with replace=true
     *
     * @param docId
     * @param score
     * @param fields
     * @return true on success
     */
    boolean replaceDocument(String docId, double score, Map<String, Object> fields);

    /**
     * replaceDocument is a convenience for calling addDocument with replace=true
     *
     * @param docId
     * @param score
     * @param fields
     * @param filter updates the document only if a boolean expression applies to the document
     * @return true on success
     */
    boolean replaceDocument(String docId, double score, Map<String, Object> fields, String filter);


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
    boolean updateDocument(String docId, double score, Map<String, Object> fields);

    /**
     * Replace specific fields in a document. Unlike #replaceDocument(), fields not present in the field list
     * are not erased, but retained. This avoids reindexing the entire document if the new values are not
     * indexed (though a reindex will happen
     *
     * @param docId  the id of the document. It cannot belong to a document already in the index unless replace is set
     * @param score  the document's score, floating point number between 0 and 1
     * @param fields a map of the document's fields
     * @param filter updates the document only if a boolean expression applies to the document
     * @return true on success
     */
    boolean updateDocument(String docId, double score, Map<String, Object> fields, String filter);

    /**
     * Index a document already in redis as a HASH key.
     *
     * @param docId   the id of the document in redis. This must match an existing, unindexed HASH key
     * @param score   the document's index score, between 0 and 1
     * @param replace if set, and the document already exists, we reindex and update it
     * @return true on success
     */
    boolean addHash(String docId, double score, boolean replace);

    /**
     * Get the index info, including memory consumption and other statistics.
     *
     * @return a map of key/value pairs
     */
    Map<String, Object> getInfo();

    /**
     * Delete a document from the index (doesn't delete the document).
     *
     * @param docId the document's id
     * @return true if it has been deleted, false if it did not exist
     * 
     * @see #deleteDocument(String, boolean) 
     */
    boolean deleteDocument(String docId);
    
    /**
     * Delete a documents from the index
     *
     * @param deleteDocuments  if <code>true</code> also deletes the actual document ifs it is in the index
     * @param docIds the document's ids
     * @return true on success for each document if it has been deleted, false if it did not exist
     */
    boolean[] deleteDocuments(boolean deleteDocuments, String... docIds);

    /**
     * Delete a document from the index.
     *
     * @param docId the document's id
     * @param deleteDocument if <code>true</code> also deletes the actual document if it is in the index
     * @return true if it has been deleted, false if it did not exist
     */
    boolean deleteDocument(String docId, boolean deleteDocument);

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
    Document getDocument(String docId);   

    /**
     * Get a document from the index
     *
     * @param docId The document ID to retrieve
     * @param decode <code>false</code> - keeps the fields value as byte[] 
     * @return The document as stored in the index. If the document does not exist, null is returned.
     */
    Document getDocument(String docId, boolean decode);

    /**
     * Get a document from the index
     *
     * @param docIds The document IDs to retrieve 
     * @return The documents stored in the index. If a document does not exist, null is returned.
     */
    List<Document> getDocuments(String ...docIds);

    /**
     * Get a documents from the index
     *
     * @param decode <code>false</code> - keeps the fields value as byte[] 
     * @param docIds The document IDs to retrieve
     * @return The documents stored in the index. If a document does not exist, null is returned.
     */
    List<Document> getDocuments(boolean decode, String ...docIds);
    
    /**
     * Drop the index and all associated keys, including documents
     *
     * @return true on success
     */
    boolean dropIndex();
    
    /**
     * Drop the index and all associated keys, including documents
     *
     * @return true on success
     */
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

    /**
     * Deletes a string from a suggestion
     * @return 1 if the string was found and deleted, 0 otherwise.
     */
    Long deleteSuggestion(String entry);

    /**
     * Gets the size of an auto-complete suggestion
     * @return the size of an auto-complete suggestion dictionary
     */
    Long getSuggestionLength();

    /**
     * Alter index add fields
     *
     * @param fields list of fields
     * @return true if successful
     */
    boolean alterIndex(Field ...fields);

    /**
     * Set runtime configuration option
     *
     * @param option the name of the configuration option
     * @param value a value for the configuration option
     * @return
     */
    boolean setConfig(ConfigOption option, String value);

    /**
     * Get runtime configuration option value
     *
     * @param option the name of the configuration option
     * @return
     */
    String getConfig(ConfigOption option);

    /**
     * Get all configuration options, consisting of the option's name and current value
     *
     * @return
     */
    Map<String, String> getAllConfig();
    
    /**
     * Adds a synonym group.
     * 
     * @param terms
     * 
     * @return the synonym group id
     */
    long addSynonym(String ...terms);
      
    /**
     * Updates a synonym group.
     * 
     * @param synonymGroupId
     * @param terms
     * 
     * @return true on success
     */
    boolean updateSynonym(long synonymGroupId, String ...terms);
       
    /**
     * @return a map of synonym terms and their synonym group ids.
     */
    Map<String, List<Long>> dumpSynonym();
}
