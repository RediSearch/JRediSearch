package io.redisearch.client;

import java.util.List;

import io.redisearch.Keywords;

/**
 * IndexDefinition encapsulates configuration for index definition creation 
 * and should be given to the client on index creation
 */
public class IndexDefinition {
  
  public enum Type {
    HASH
  }
  
  private Type type = Type.HASH;
  private boolean async = false; 
  private String[] prefixes;
  private String filter;
  private String languageField;
  private String language;
  private String scoreFiled;
  private double score = 1.0; // Default score when score isn't defined 
  private String payloadField;
  
  public Type getType() {
    return type;
  }
  
  public IndexDefinition setType(Type type) {
    this.type = type;
    return this;    
  }
  
  public boolean isAsync() {
    return async;
  }
  public IndexDefinition setAsync(boolean async) {
    this.async = async;
    return this;    
  }
  
  public String[] getPrefixes() {
    return prefixes;
  }
  
  public IndexDefinition setPrefixes(String... prefixes) {
    this.prefixes = prefixes;
    return this;    
  }
  
  public String getFilter() {
    return filter;
  }
  
  public IndexDefinition setFilter(String filter) {
    this.filter = filter;
    return this;    
  }
  
  public String getLanguageField() {
    return languageField;
  }
  
  public IndexDefinition setLanguageField(String languageField) {
    this.languageField = languageField;
    return this;    
  }
  public String getLanguage() {
    return language;
  }
  
  public IndexDefinition setLanguage(String language) {
    this.language = language;
    return this;    
  }
  
  public String getScoreFiled() {
    return scoreFiled;
  }
  
  public IndexDefinition setScoreFiled(String scoreFiled) {
    this.scoreFiled = scoreFiled;
    return this;    
  }
  
  public double getScore() {
    return score;
  }
  
  public IndexDefinition setScore(double score) {
    this.score = score;
    return this;
  }

  public String getPayloadField() {
    return payloadField;
  }
  
  public IndexDefinition setPayloadField(String payloadField) {
    this.payloadField = payloadField;
    return this;
  }

  public void serializeRedisArgs(List<String> args) {
    if (type != null) {
      args.add(Keywords.ON.name());
      args.add(type.name());
      
      if (async) {
        args.add(Keywords.ASYNC.name());
      }

      if (prefixes != null && prefixes.length>0) {
        args.add(Keywords.PREFIX.name());
        args.add(Integer.toString(prefixes.length));
        for(String prefix : prefixes) {
          args.add(prefix);
        }
      }
      
      if (filter != null) {
        args.add(Keywords.FILTER.name());
        args.add(filter);
      }
      
      if (languageField != null) {
        args.add(Keywords.LANGUAGE_FIELD.name());
        args.add(languageField);      
      }
      
      if (language != null) {
        args.add(Keywords.LANGUAGE.name());
        args.add(language);      
      }
      
      if (scoreFiled != null) {
        args.add(Keywords.SCORE_FIELD.name());
        args.add(scoreFiled);      
      }
      
      if (score != 1.0) {
        args.add(Keywords.SCORE.name());
        args.add(Double.toString(score));      
      }
      
      if (payloadField != null) {
        args.add(Keywords.PAYLOAD_FIELD.name());
        args.add(payloadField);      
      }
    }
  }
}