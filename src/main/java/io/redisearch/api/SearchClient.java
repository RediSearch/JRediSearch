package io.redisearch.api;

import io.redisearch.AggregationResult;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.aggregation.AggregationRequest;

public interface SearchClient {

    SearchResult search(Query q);

    AggregationResult aggregate(AggregationRequest q);

    String explain(Query q);

}
