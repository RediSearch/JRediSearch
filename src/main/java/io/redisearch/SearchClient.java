package io.redisearch;

import io.redisearch.aggregation.AggregationRequest;
import io.redisearch.client.AddOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface SearchClient {
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

    Long addSuggestion(Suggestion suggestion, boolean increment);

    List<String> getSuggestion(String prefix, boolean withPayloads, int max, boolean fuzzy, boolean scores);


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
