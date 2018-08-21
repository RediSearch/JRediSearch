package redis.clients.rdbc;

import java.io.Closeable;

public interface Pool<T> extends Closeable {

    T getResource();
}
