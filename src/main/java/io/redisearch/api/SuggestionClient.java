package io.redisearch.api;

import io.redisearch.Suggestion;
import io.redisearch.client.SuggestionOptions;

import java.util.List;

public interface SuggestionClient {
    String INCREMENT_FLAG = "INCR";
    String PAYLOAD_FLAG = "PAYLOAD";
    String MAX_FLAG = "MAX";
    String FUZZY_FLAG = "FUZZY";

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
     *
     * @param prefix            the partial word
     * @param suggestionOptions the options on what you need returned and other usage
     * @return list of suggestions
     */
    List<Suggestion> getSuggestion(String prefix, SuggestionOptions suggestionOptions);
}
