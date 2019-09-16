package redisearch.client;

import io.redisearch.Document;
import io.redisearch.Schema;
import io.redisearch.client.Client;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.ThreadParams;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Each thread will have a dedicated JRediSearch client.
 */
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class DocumentIngestionBenchmark {

    static final Logger LOG = LoggerFactory.getLogger(Main.class);
    static final int RANDOM_SEED = 12345;
    static final int JREDISCLIENT_TIMEOUT = 500;
    static final int JREDISCLIENT_POOLSIZE = 100;
    static final int MULTIDOCUMENT_SIZE = 10;
    static final int DOCS_SIZE = 10000;
    static final String INDEX_NAME = "indexName";
    static final String HOST_IP = "127.0.0.1";
    static final int HOST_PORT = 6379;

    static Options opt;

    private io.redisearch.client.Client client;
    private Jedis jedis_client;

    /*
     * ============================== HOW TO RUN THIS TEST: ====================================
     *
     * You can run this test:
     *
     * a) Via the command line:
     *    $ mvn clean install
     *    $ java -jar target/jredisearch-1.1.0-SNAPSHOT-perf-tests.jar -wi 1 -i 5 -t 16 -f 5
     *    (we requested 1 warmup iterations, 5 iterations, 8 threads, and 5 forks)
     *
     */
    public static void main(String[] args) throws RunnerException {

        opt = new OptionsBuilder()
                .include(DocumentIngestionBenchmark.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(5)
                .threads(4)
                .forks(5)
                .build();

        new Runner(opt).run();

    }

    @Setup
    public void setup(BenchmarkParams params) {

        client = new Client(INDEX_NAME, HOST_IP, HOST_PORT, JREDISCLIENT_TIMEOUT, JREDISCLIENT_POOLSIZE);
        jedis_client = new Jedis(HOST_IP, HOST_PORT);
        jedis_client.flushAll();
        client.dropIndex(true);

        LOG.info("Started Creating Schema");
        Schema sc = new Schema();
        sc.addSortableTextField("name", 1.0);
        sc.addSortableNumericField("count");
        sc.addSortableNumericField("height");
        sc.addSortableNumericField("height2");
        sc.addSortableNumericField("height3");
        sc.addSortableNumericField("height4");
        sc.addSortableNumericField("height5");
        sc.addSortableNumericField("height6");
        sc.addSortableNumericField("height7");
        sc.addSortableNumericField("height8");
        sc.addSortableNumericField("height9");
        sc.addSortableNumericField("height10");
        sc.addSortableNumericField("height11");
        sc.addSortableNumericField("height12");
        client.createIndex(sc, Client.IndexOptions.defaultOptions());
        LOG.info("Finished Creating Schema");
        client.close();


    }

    @TearDown(Level.Trial)
    public void doTearDown() {
        //clean the db after the benchmark
        jedis_client = new Jedis(HOST_IP, HOST_PORT, 30000);
        jedis_client.flushAll();
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public void Client_addDocument(NumericValues numv) {
        numv.client.addDocument(new Document(String.format("multidoc:thread%d:%d", numv.id, numv.docPos), numv.fieldsList.get(numv.random.nextInt(DOCS_SIZE)))
        );
        numv.docPos++;
    }

    @Benchmark
    @OperationsPerInvocation(MULTIDOCUMENT_SIZE)
    public void Client_addDocuments(NumericValues numv) {
        List<Document> docs = new ArrayList<Document>();
        for (int i = 0; i < MULTIDOCUMENT_SIZE; i++) {
            docs.add(new Document(String.format("multidoc:thread%d:%d", numv.id, numv.docPos), numv.fieldsList.get(numv.random.nextInt(DOCS_SIZE))));
            numv.docPos++;
        }
        numv.client.addDocuments(docs.toArray(new Document[docs.size()]));

    }

    @State(Scope.Thread)
    public static class NumericValues {
        Random random;
        private int id;
        private io.redisearch.client.Client client;
        private int docPos;
        private List<Map<String, Object>> fieldsList;


        @Setup
        public void setup(ThreadParams threads) {
            client = new Client(INDEX_NAME, HOST_IP, HOST_PORT, JREDISCLIENT_TIMEOUT, JREDISCLIENT_POOLSIZE);
            id = threads.getThreadIndex();
            random = new Random();
            random.setSeed(RANDOM_SEED);
            docPos = 0;
            LOG.info("Started Creating FieldsList");

            fieldsList = new ArrayList<Map<String, Object>>();
            for (int i = 0; i < DOCS_SIZE; i++) {
                Map<String, Object> fields = new HashMap<>();

                fields.put("count", random.nextFloat());
                fields.put("height", random.nextFloat());
                fields.put("height2", random.nextFloat());
                fields.put("height3", random.nextFloat());
                fields.put("height4", random.nextFloat());
                fields.put("height5", random.nextFloat());
                fields.put("height6", random.nextFloat());
                fields.put("height7", random.nextFloat());
                fields.put("height8", random.nextFloat());
                fields.put("height9", random.nextFloat());
                fields.put("height10", random.nextFloat());
                fields.put("height11", random.nextFloat());
                fields.put("height12", random.nextFloat());
                fieldsList.add(fields);
            }
            LOG.info("Finished Creating FieldsList");

        }

        @TearDown(Level.Trial)
        public void doTearDown() {
        }


    }


}
