package io.redisearch;

import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface Client {
    boolean createIndex(Schema schema, IndexOptions options);

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


    /**
     * IndexOptions encapsulates flags for index creation and shuold be given to the client on index creation
     */
    class IndexOptions {
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
         *
         * @param flags flag mask
         */
        public IndexOptions(int flags) {
            this.flags = flags;
            stopwords = null;
        }

        /**
         * The default indexing options - use term offsets and keep fields flags
         */
        public static IndexOptions Default() {
            return new IndexOptions(DEFAULT_FLAGS);
        }

        /**
         * Set a custom stopword list
         *
         * @return the options object itself, for builder-style construction
         */
        public IndexOptions SetStopwords(String... stopwords) {
            this.stopwords = Arrays.asList(stopwords);
            return this;
        }

        /**
         * Set the index to contain no stopwords, overriding the default list
         *
         * @return the options object itself, for builder-style constructions
         */
        public IndexOptions SetNoStopwords() {
            stopwords = new ArrayList<>(0);
            return this;
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

            if (stopwords != null) {

                args.add("STOPWORDS");
                args.add(String.format("%d", stopwords.size()));
                if (stopwords.size() > 0) {
                    args.addAll(stopwords);
                }

            }
        }
    }
}
