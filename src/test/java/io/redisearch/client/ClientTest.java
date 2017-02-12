package io.redisearch.client;

import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.Schema;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;


/**
 * Created by dvirsky on 09/02/17.
 */
public class ClientTest {
    @Test
    public void search() throws Exception {
        Client cl = new Client("testung", "localhost", 6379);

        Schema sc = new Schema().addTextField("title", 1.0).addTextField("body", 1.0);

        assertTrue(cl.createIndex(sc, Client.IndexOptions.Default()));

        Map<String, Object> doc = new HashMap<>();
        doc.put("title", "hello world");
        doc.put("body", "lorem ipsum");

        assertTrue(cl.addDocument("doc1", doc));

        List<Document> res = cl.search(new Query("hello world"));

        for (Document d : res) {
            System.out.println(d);
        }

    }

}