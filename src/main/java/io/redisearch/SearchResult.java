package io.redisearch;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dvirsky on 19/02/17.
 */
public class SearchResult {
    public long totalResults;
    public final List<Document> docs;


    public SearchResult(List<Object> resp, boolean hasContent, boolean hasScores, boolean hasPayloads) {

        // Calculate the step distance to walk over the results.
        // The order of results is id, score (if withScore), payLoad (if hasPayloads), fields
        int step = 1;
        if (hasScores) {
            step += 1;
        }
        if (hasContent) {
            step += 1;
            if (hasPayloads) {
                step += 1;
            }
        }

        // the first element is always the number of results
        totalResults = (Long) resp.get(0);
        docs = new ArrayList<>(resp.size() - 1);

        for (int i = 1; i < resp.size(); i += step) {

            String id = new String((byte[]) resp.get(i));
            Double score = 1.0;
            byte[] payload = null;
            List fields = null;
            if (hasScores) {
                score = Double.valueOf(new String((byte[]) resp.get(i + 1)));
            }
            if (hasPayloads) {
                payload = (byte[]) resp.get(i + 2);
            }

            if (hasContent) {
                fields = (List) resp.get(i + 1);
            }

            docs.add(Document.load(id, score, payload, fields));
        }


    }
}
