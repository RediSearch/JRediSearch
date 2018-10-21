[![CircleCI](https://circleci.com/gh/RedisLabs/JRediSearch/tree/master.svg?style=svg)](https://circleci.com/gh/RedisLabs/JRediSearch/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisearch/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.redislabs/jredisearch)


# JRediSearch

A Java Client Library for [RediSearch](https://oss.redislabs.com/redisearch/)

## Overview 

This project contains a Java library abstracting the API of the RediSearch Redis module, that implements a powerful 
in-memory search engine inside Redis. 
 
## Installing

JRediSearch is available using the maven central snapshot repository and via official
releases.

### Official Releases

```xml
  <dependencies>
    <dependency>
      <groupId>com.redislabs</groupId>
      <artifactId>jredisearch</artifactId>
      <version>0.12.0</version>
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
      <version>0.13.0-SNAPSHOT</version>
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

client.createIndex(sc, Client.IndexOptions.Default());

```
 
Adding documents to the index:

```java
Map<String, Object> fields = new HashMap<>();
fields.put("title", "hello world");
fields.put("body", "lorem ipsum");
fields.put("price", 1337);

client.addDocument("doc1", fields);

```

Searching the index:

```java

// Creating a complex query
Query q = new Query("hello world")
                    .addFilter(new Query.NumericFilter("price", 0, 1000))
                    .limit(0,5);

// actual search
SearchResult res = client.search(q);


```

---
 
### Also supported:

* Geo filtering
* Exact matching
* Union matching
* Stemming in 17 languages
* Deleting and updating documents on the fly
* And much more... See [https://oss.redislabs.com/redisearch/](https://oss.redislabs.com/redisearch/) for more details.

