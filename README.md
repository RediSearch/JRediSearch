[![license](https://img.shields.io/github/license/RedisLabs/JRediSearch.svg)](https://github.com/RediSearch/JRediSearch/blob/master/LICENSE)
[![CircleCI](https://circleci.com/gh/RediSearch/JRediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/JRediSearch/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RedisLabs/JRediSearch.svg)](https://github.com/RedisLabs/JRediSearch/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisearch/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisearch)
[![Javadocs](https://www.javadoc.io/badge/com.redislabs/jredisearch.svg)](https://www.javadoc.io/doc/com.redislabs/jredisearch)
[![Codecov](https://codecov.io/gh/RediSearch/JRediSearch/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/JRediSearch)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/RediSearch/JRediSearch.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/RediSearch/JRediSearch/context:java)
[![Known Vulnerabilities](https://snyk.io/test/github/RediSearch/JRediSearch/badge.svg?targetFile=pom.xml)](https://snyk.io/test/github/RediSearch/JRediSearch?targetFile=pom.xml)

# JRediSearch
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)
[![Discord](https://img.shields.io/discord/697882427875393627?style=flat-square)](https://discord.gg/xTbqgTB)

A Java Client Library for [RediSearch](https://oss.redislabs.com/redisearch/)

## Deprecation notice

As of [jedis 4.0.0](https://github.com/redis/jedis) this library is deprecated. It's features have been merged into jedis. Please either install it [from maven](https://mvnrepository.com/artifact/redis.clients/jedis) or [the repo](https://github.com/redis/jedis).

## Overview 

This project contains a Java library abstracting the API of the RediSearch Redis module, that implements a powerful 
in-memory Secondary Index, Query Engine and Full-Text Search engine inside Redis. 
 
## Installing

JRediSearch is available using the maven central snapshot repository and via official
releases.

### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisearch</artifactId>
      <version>2.0.0</version>
    </dependency>
  </dependencies>
```

### Snapshots

```xml
  <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
  </repositories>
```

and
```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisearch</artifactId>
      <version>2.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

## Usage example

Initializing the client with JedisPool:

```java
import io.redisearch.client.Client;
import io.redisearch.Document;
import io.redisearch.SearchResult;
import io.redisearch.Query;
import io.redisearch.Schema;

...

Client client = new Client("testung", "localhost", 6379);
```
Initializing the client with JedisSentinelPool:

```java
import io.redisearch.client.Client;
import io.redisearch.Document;
import io.redisearch.SearchResult;
import io.redisearch.Query;
import io.redisearch.Schema;

...

private static final String MASTER_NAME = "mymaster";
private static final Set<String> sentinels;
static {
    sentinels = new HashSet();
    sentinels.add("localhost:7000");
    sentinels.add("localhost:7001");
    sentinels.add("localhost:7002");
}

...

Client client = new Client("testung", MASTER_NAME, sentinels);
```

Defining a schema for an index and creating it:

```java
Schema sc = new Schema()
                .addTextField("title", 5.0)
                .addTextField("body", 1.0)
                .addNumericField("price");

// IndexDefinition requires RediSearch 2.0+
IndexDefinition def = new IndexDefinition()
						.setPrefixes(new String[] {"item:", "product:"})
						.setFilter("@price>100");

client.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(def));
```
 
Adding documents to the index:

```java
Map<String, Object> fields = new HashMap<>();
fields.put("title", "hello world");
fields.put("state", "NY");
fields.put("body", "lorem ipsum");
fields.put("price", 1337);

// RediSearch 2.0+ supports working with Redis Hash commands
try(Jedis conn = client.connection()){
	conn.hset("item", fields);
}
```

```java
// Prior to RediSearch 2.0+ the addDocument has to be called
client.addDocument("item", fields);
```

Searching the index:

```java
// Creating a complex query
Query q = new Query("hello world")
                    .addFilter(new Query.NumericFilter("price", 0, 1000))
                    .limit(0,5);

// actual search
SearchResult res = client.search(q);

// aggregation query
AggregationBuilder r = new AggregationBuilder("hello")
  .apply("@price/1000", "k")
  .groupBy("@state", Reducers.avg("@k").as("avgprice"))
  .filter("@avgprice>=2")
  .sortBy(10, SortedField.asc("@state"));
  
AggregationResult res = client.aggregate(r);
```

---
 
### Also supported:

* Geo filtering
* Exact matching
* Union matching
* Stemming in 17 languages
* Deleting and updating documents on the fly
* And much more... See [https://oss.redislabs.com/redisearch/](https://oss.redislabs.com/redisearch/) for more details.

