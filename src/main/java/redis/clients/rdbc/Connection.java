package redis.clients.rdbc;


import java.io.Closeable;
import java.util.Map;
import java.util.Set;


public interface Connection extends Closeable {
    BinaryClient getClient();

    @Override
    void close();

    void flushDB();

    String hmset(String key, Map<String, String> hash);

    Set<String> keys(String pattern);

    String ping(String message);
}
