package io.redisearch.client;

/**
 * Created by mnunberg on 2/15/18.
 *
 * This class represents options which may be passed when adding a document
 */
public class AddOptions {
    public enum ReplacementPolicy {
        /** The default mode. This will cause the add operation to fail if the document already exists */
        NONE,

        /**
         * Replace/reindex the entire document. This has the effect of atomically deleting the previous
         * document and replacing it with the context of the new document. Fields in the old document which
         * are not present in the new document are lost
         */
        FULL,

        /**
         * Only reindex/replace fields that are updated in the command. Fields in the old document which are
         * not present in the new document are preserved. Fields that are present in both are overwritten by
         * the new document
         */
        PARTIAL
    }

    private String language = null;
    private boolean save = true;
    private ReplacementPolicy replacementPolicy = ReplacementPolicy.NONE;

    /**
     * Create a new DocumentOptions object. Methods can later be chained via a builder-like pattern
     */
    public AddOptions() {
    }

    /**
     * Set the indexing language
     * @param language The language the document should be stemmed as.
     * @return the {@link AddOptions} object for further options
     */
    public AddOptions setLanguage(String language) {
        this.language = language;
        return this;
    }

    /**
     * Indicate that the document's contents should not be stored in the database. This saves disk/memory space on the
     * server but prevents retrieving the document itself.
     * @return the {@link AddOptions} object for further options
     */
    public AddOptions setNosave() {
        return setNosave(true);
    }

    /**
     * Whether document's contents should not be stored in the database.
     * @param enabled if enabled, the document is <b>not</b> stored on the server. This saves disk/memory space on the
     * server but prevents retrieving the document itself.
     * @return the {@link AddOptions} object for further options
     */
    public AddOptions setNosave(boolean enabled) {
        this.save = !enabled;
        return this;
    }

    /**
     * Indicate the behavior for the existing document.
     * @param mode One of the replacement modes
     * @return the {@link AddOptions} object for further options
     */
    public AddOptions setReplacementPolicy(ReplacementPolicy mode) {
        this.replacementPolicy = mode;
        return this;
    }

    boolean getNosave() {
        return !this.save;
    }

    ReplacementPolicy getReplacementPolicy() {
        return replacementPolicy;
    }

    String getLanguage() {
        return language;
    }
}
