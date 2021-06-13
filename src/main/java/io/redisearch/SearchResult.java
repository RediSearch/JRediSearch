package io.redisearch;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.BuilderFactory;

/**
 * SearchResult encapsulates the returned result from a search query.
 * It contains publicly accessible fields for the total number of results, and an array of {@link Document}
 * objects containing the actual returned documents.
 */
public class SearchResult {
    public final long totalResults;
    public final List<Document> docs;

    public SearchResult(List<Object> resp, boolean hasContent, boolean hasScores, boolean hasPayloads) {
      this(resp, hasContent, hasScores, hasPayloads, true);
    }
    
    public SearchResult(List<Object> resp, boolean hasContent, boolean hasScores, boolean hasPayloads, boolean decode) {

        // Calculate the step distance to walk over the results.
        // The order of results is id, score (if withScore), payLoad (if hasPayloads), fields
        int step = 1;
        int scoreOffset = 0;
        int contentOffset = 1;
        int payloadOffset = 0;
        if (hasScores) {
            step += 1;
            scoreOffset = 1;
            contentOffset+=1;
        }
        if (hasContent) {
            step += 1;
            if (hasPayloads) {
                payloadOffset =scoreOffset+1;
                step += 1;
                contentOffset+=1;
            }
        }

        // the first element is always the number of results
        totalResults = (Long) resp.get(0);
        docs = new ArrayList<>(resp.size() - 1);

        for (int i = 1; i < resp.size(); i += step) {
        	
            double score = hasScores ? BuilderFactory.DOUBLE.build(resp.get(i + scoreOffset)) : 1.0;           
            byte[] payload = hasPayloads ? (byte[]) resp.get(i + payloadOffset) : null;
            List<byte[]> fields = hasContent ? (List<byte[]>) resp.get(i + contentOffset) : null; 
            String id = new String((byte[]) resp.get(i));
            
            docs.add(Document.load(id, score, payload, fields, decode));
        }


    }
}
