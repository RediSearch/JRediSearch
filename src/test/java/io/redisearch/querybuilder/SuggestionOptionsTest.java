package io.redisearch.querybuilder;

import io.redisearch.client.SuggestionOptions;

import org.junit.Test;
import static org.junit.Assert.*;

public class SuggestionOptionsTest {

  @Test
  public void testSuggestionOptionsBuilder() {
    SuggestionOptions options = 
        SuggestionOptions.builder()
        .max(3)
        .fuzzy()
        .with(SuggestionOptions.With.PAYLOAD)
        .build();
    
    assertEquals(3, options.getMax());
    assertEquals(true, options.isFuzzy());
    assertEquals(SuggestionOptions.With.PAYLOAD, options.getWith().get());
  }
  
  @Test
  public void testSuggestionOptionsToBuilder() {
    
    SuggestionOptions options = SuggestionOptions.builder()
        .max(3)
        .fuzzy()
        .with(SuggestionOptions.With.PAYLOAD)
        .build()
        .toBuilder()
        .max(4)
        .build();
        
    assertEquals(4, options.getMax());
    assertEquals(true, options.isFuzzy());
    assertEquals(SuggestionOptions.With.PAYLOAD, options.getWith().get());
  }
}
