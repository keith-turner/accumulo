/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.clientImpl;

import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class ThriftTransportPool {

  private static final SecureRandom random = new SecureRandom();
  private long killTime = 1000 * 3;

  private static class CachedConnections {
    Deque<CachedConnection> unreserved = new ArrayDeque<>(); // stack - LIFO
    Map<CachedTTransport,CachedConnection> reserved = new HashMap<>();

    public CachedConnection reserveAny() {
      CachedConnection cachedConnection = unreserved.poll(); // safe pop
      if (cachedConnection != null) {
        cachedConnection.reserve();
        reserved.put(cachedConnection.transport, cachedConnection);
        if (log.isTraceEnabled()) {
          log.trace("Using existing connection to {}", cachedConnection.transport.cacheKey);
        }
      }
      return cachedConnection;
    }

    private void removeExpiredConnections(final ArrayList<CachedConnection> expired, final long killTime) {
      long currTime = System.currentTimeMillis();
      while (isLastUnreservedExpired(currTime, killTime)) {
        expired.add(unreserved.removeLast());
      }
    }

    boolean isLastUnreservedExpired(final long currTime, final long killTime) {
      return !unreserved.isEmpty() && (currTime - unreserved.peekLast().lastReturnTime) > killTime;
    }

    void checkReservedForStuckIO() {
      reserved.values().forEach(c -> c.transport.checkForStuckIO(STUCK_THRESHOLD));
    }

    void closeAllTransports() {
      closeTransports(unreserved);
      closeTransports(reserved.values());
    }

    void closeTransports(final Iterable<CachedConnection> stream) {
      stream.forEach((connection) -> {
        try {
          connection.transport.close();
        } catch (Exception e) {
          log.debug("Error closing transport during shutdown", e);
        }
      });
    }

    CachedConnection removeReserved(CachedTTransport transport) {
      return reserved.remove(transport);
    }
  }

  private static class ConnectionPool {
    final ConcurrentHashMap<ThriftTransportKey,ReentrantLock> locks = new ConcurrentHashMap<>();
    final ConcurrentHashMap<ThriftTransportKey,CachedConnections> connections = new ConcurrentHashMap<>();

    Set<ThriftTransportKey> getThriftTransportKeys() {
      return connections.keySet();
    }

    CachedConnection reserveAny(final ThriftTransportKey key) {
      return executeWithinLock(key, () -> getOrCreateCachedConnections(key).reserveAny());
    }

    CachedConnection reserveAnyIfPresent(final ThriftTransportKey key) {
      return executeWithinLock(key, () -> connections.containsKey(key)
          ? connections.get(key).reserveAny()
          : null);
    }

    void putReserved(final ThriftTransportKey key, final CachedConnection connection) {
      executeWithinLock(key, () -> getOrCreateCachedConnections(key).reserved.put(
          connection.transport, connection));
    }

    boolean returnTransport(final CachedTTransport transport, final List<CachedConnection> toBeClosed) {
      return executeWithinLock(transport.getCacheKey(), () -> unreserveConnection(transport, toBeClosed));
    }

    void shutdown() {
      for (Entry<ThriftTransportKey,CachedConnections> entry : connections.entrySet()) {
        Lock lock = locks.get(entry.getKey());
        // Explicitly do not release the lock afterwards in order to prevent other threads from
        // obtaining it during shutdown. This pool instance is expected to be nullified after this
        // method is called.
        lock.lock();

        // Close the transports.
        entry.getValue().closeAllTransports();
      }
    }

    <T> T executeWithinLock(final ThriftTransportKey key, Supplier<T> function) {
      Lock lock = getLock(key);
      try {
        return function.get();
      } finally {
        lock.unlock();
      }
    }

    void executeWithinLock(final ThriftTransportKey key, Consumer<ThriftTransportKey> consumer) {
      Lock lock = getLock(key);
      try {
        consumer.accept(key);
      } finally {
        lock.unlock();
      }
    }

    ReentrantLock getLock(final ThriftTransportKey key){
      return locks.computeIfAbsent(key, k -> new ReentrantLock());
    }

    CachedConnections getCachedConnections(final ThriftTransportKey key) {
      return connections.get(key);
    }

    CachedConnections getOrCreateCachedConnections(final ThriftTransportKey key) {
      return connections.computeIfAbsent(key, k -> new CachedConnections());
    }

    boolean unreserveConnection(final CachedTTransport transport,
        final List<CachedConnection> toBeClosed) {
      CachedConnections connections = getCachedConnections(transport.getCacheKey());
      if (connections != null) {
        CachedConnection connection = connections.removeReserved(transport);
        if (connection != null) {
          if (transport.sawError) {
            unreserveConnectionAndClearUnreserved(connections, connection, toBeClosed);
          } else {
            returnConnectionToUnreserved(connections, connection);
          }
          return true;
        }
      }
      return false;
    }

    void unreserveConnectionAndClearUnreserved(final CachedConnections connections,
        final CachedConnection connection, final List<CachedConnection> toBeClosed) {
      toBeClosed.add(connection);
      connection.unreserve();
      // Remove all unreserved cached connection when a sever has an error, not just the
      // connection that was returned.
      toBeClosed.addAll(connections.unreserved);
      connections.unreserved.clear();
    }

    void returnConnectionToUnreserved(final CachedConnections connections,
        final CachedConnection connection) {
      log.trace("Returned connection {} ioCount: {}", connection.transport.getCacheKey(),
          connection.transport.ioCount);
      connection.lastReturnTime = System.currentTimeMillis();
      connection.unreserve();
      // Using LIFO ensures that when the number of pooled connections exceeds the working
      // set size that the idle times at the end of the list grow. The connections with
      // large idle times will be cleaned up. Using a FIFO could continually reset the idle
      // times of all connections, even when there are more than the working set size.
      connections.unreserved.push(connection);
    }

    List<CachedConnection> removeExpiredConnections(final long killTime) {
      ArrayList<CachedConnection> expired = new ArrayList<>();
      for (Entry<ThriftTransportKey,CachedConnections> entry : connections.entrySet()) {
        CachedConnections connections = entry.getValue();
        executeWithinLock(entry.getKey(), (key) -> {
          connections.removeExpiredConnections(expired, killTime);
          connections.checkReservedForStuckIO();
        });
      }
      return expired;
    }
  }

  private volatile ConnectionPool connectionPool = new ConnectionPool();

  private Map<ThriftTransportKey,Long> errorCount = new HashMap<>();
  private Map<ThriftTransportKey,Long> errorTime = new HashMap<>();
  private Set<ThriftTransportKey> serversWarnedAbout = new HashSet<>();

  private Thread checkThread;

  private static final Logger log = LoggerFactory.getLogger(ThriftTransportPool.class);

  private static final Long ERROR_THRESHOLD = 20L;
  private static final int STUCK_THRESHOLD = 2 * 60 * 1000;

  private static class CachedConnection {

    public CachedConnection(CachedTTransport t) {
      this.transport = t;
    }

    void reserve() {
      Preconditions.checkState(!this.transport.reserved);
      this.transport.setReserved(true);
    }

    void unreserve() {
      Preconditions.checkState(this.transport.reserved);
      this.transport.setReserved(false);
    }

    final CachedTTransport transport;

    long lastReturnTime;
  }

  public static class TransportPoolShutdownException extends RuntimeException {
    public TransportPoolShutdownException(String msg) {
      super(msg);
    }

    private static final long serialVersionUID = 1L;
  }

  private static class Closer implements Runnable {
    final ThriftTransportPool pool;

    public Closer(ThriftTransportPool pool) {
      this.pool = pool;
    }

    private void closeConnections() throws InterruptedException {
      while (true) {
        pool.closeExpiredConnections();
        Thread.sleep(500);
      }
    }

    @Override
    public void run() {
      try {
        closeConnections();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (TransportPoolShutdownException e) {}
    }
  }

  static class CachedTTransport extends TTransport {

    private ThriftTransportKey cacheKey;
    private TTransport wrappedTransport;
    private boolean sawError = false;

    private volatile String ioThreadName = null;
    private volatile long ioStartTime = 0;
    private volatile boolean reserved = false;

    private String stuckThreadName = null;

    int ioCount = 0;
    int lastIoCount = -1;

    private void sawError() {
      sawError = true;
    }

    final void setReserved(boolean reserved) {
      this.reserved = reserved;
      if (reserved) {
        ioThreadName = Thread.currentThread().getName();
        ioCount = 0;
        lastIoCount = -1;
      } else {
        if ((ioCount & 1) == 1) {
          // connection unreserved, but it seems io may still be happening
          log.warn("Connection returned to thrift connection pool that may still be in use {} {}",
              ioThreadName, Thread.currentThread().getName(), new Exception());
        }

        ioCount = 0;
        lastIoCount = -1;
        ioThreadName = null;
      }
      checkForStuckIO(STUCK_THRESHOLD);
    }

    final void checkForStuckIO(long threshold) {
      // checking for stuck io needs to be light weight.

      // Tried to call System.currentTimeMillis() and Thread.currentThread() before every io
      // operation.... this dramatically slowed things down. So switched to
      // incrementing a counter before and after each io operation.

      if ((ioCount & 1) == 1) {
        // when ioCount is odd, it means I/O is currently happening
        if (ioCount == lastIoCount) {
          // still doing same I/O operation as last time this
          // functions was called
          long delta = System.currentTimeMillis() - ioStartTime;
          if (delta >= threshold && stuckThreadName == null) {
            stuckThreadName = ioThreadName;
            log.warn("Thread \"{}\" stuck on IO to {} for at least {} ms", ioThreadName, cacheKey,
                delta);
          }
        } else {
          // remember this ioCount and the time we saw it, need to see
          // if it changes
          lastIoCount = ioCount;
          ioStartTime = System.currentTimeMillis();

          if (stuckThreadName != null) {
            // doing I/O, but ioCount changed so no longer stuck
            log.info("Thread \"{}\" no longer stuck on IO to {} sawError = {}", stuckThreadName,
                cacheKey, sawError);
            stuckThreadName = null;
          }
        }
      } else {
        // I/O is not currently happening
        if (stuckThreadName != null) {
          // no longer stuck, and was stuck in the past
          log.info("Thread \"{}\" no longer stuck on IO to {} sawError = {}", stuckThreadName,
              cacheKey, sawError);
          stuckThreadName = null;
        }
      }
    }

    public CachedTTransport(TTransport transport, ThriftTransportKey cacheKey2) {
      this.wrappedTransport = transport;
      this.cacheKey = cacheKey2;
    }

    @Override
    public boolean isOpen() {
      return wrappedTransport.isOpen();
    }

    @Override
    public void open() throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.open();
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public int read(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        return wrappedTransport.read(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public int readAll(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        return wrappedTransport.readAll(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void write(byte[] arg0, int arg1, int arg2) throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.write(arg0, arg1, arg2);
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void write(byte[] arg0) throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.write(arg0);
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public void close() {
      try {
        ioCount++;
        wrappedTransport.close();
      } finally {
        ioCount++;
      }

    }

    @Override
    public void flush() throws TTransportException {
      try {
        ioCount++;
        wrappedTransport.flush();
      } catch (TTransportException tte) {
        sawError();
        throw tte;
      } finally {
        ioCount++;
      }
    }

    @Override
    public boolean peek() {
      try {
        ioCount++;
        return wrappedTransport.peek();
      } finally {
        ioCount++;
      }
    }

    @Override
    public byte[] getBuffer() {
      try {
        ioCount++;
        return wrappedTransport.getBuffer();
      } finally {
        ioCount++;
      }
    }

    @Override
    public int getBufferPosition() {
      try {
        ioCount++;
        return wrappedTransport.getBufferPosition();
      } finally {
        ioCount++;
      }
    }

    @Override
    public int getBytesRemainingInBuffer() {
      try {
        ioCount++;
        return wrappedTransport.getBytesRemainingInBuffer();
      } finally {
        ioCount++;
      }
    }

    @Override
    public void consumeBuffer(int len) {
      try {
        ioCount++;
        wrappedTransport.consumeBuffer(len);
      } finally {
        ioCount++;
      }
    }

    public ThriftTransportKey getCacheKey() {
      return cacheKey;
    }

  }

  private ThriftTransportPool() {}

  public TTransport getTransport(HostAndPort location, long milliseconds, ClientContext context)
      throws TTransportException {
    return getTransport(new ThriftTransportKey(location, milliseconds, context));
  }

  private TTransport getTransport(ThriftTransportKey cacheKey) throws TTransportException {
    // compute hash code outside of lock, this lowers the time the lock is held
    cacheKey.precomputeHashCode();

    ConnectionPool pool = getConnectionPool();
    CachedConnection connection = pool.reserveAny(cacheKey);

    if (connection != null) {
      log.trace("Using existing connection to {}", cacheKey.getServer());
      return connection.transport;
    } else {
      return createNewTransport(cacheKey);
    }
  }

  @VisibleForTesting
  public Pair<String,TTransport> getAnyTransport(List<ThriftTransportKey> servers,
      boolean preferCachedConnection) throws TTransportException {

    servers = new ArrayList<>(servers);

    if (preferCachedConnection) {
      HashSet<ThriftTransportKey> serversSet = new HashSet<>(servers);

      ConnectionPool pool = getConnectionPool();

      // randomly pick a server from the connection cache
      serversSet.retainAll(pool.getThriftTransportKeys());

      if (serversSet.size() > 0) {
        ArrayList<ThriftTransportKey> cachedServers = new ArrayList<>(serversSet);
        Collections.shuffle(cachedServers, random);

        for (ThriftTransportKey ttk : cachedServers) {
          CachedConnection connection = pool.reserveAny(ttk);
          if (connection != null) {
            final String serverAddr = ttk.getServer().toString();
            log.trace("Using existing connection to {}", serverAddr);
            return new Pair<>(serverAddr, connection.transport);
          }

        }
      }
    }

    ConnectionPool pool = getConnectionPool();
    int retryCount = 0;
    while (servers.size() > 0 && retryCount < 10) {

      int index = random.nextInt(servers.size());
      ThriftTransportKey ttk = servers.get(index);

      if (preferCachedConnection) {
        CachedConnection connection = pool.reserveAnyIfPresent(ttk);
        if (connection != null) {
          return new Pair<>(ttk.getServer().toString(), connection.transport);
        }
      }

      try {
        return new Pair<>(ttk.getServer().toString(), createNewTransport(ttk));
      } catch (TTransportException tte) {
        log.debug("Failed to connect to {}", servers.get(index), tte);
        servers.remove(index);
        retryCount++;
      }
    }

    throw new TTransportException("Failed to connect to a server");
  }

  private TTransport createNewTransport(ThriftTransportKey cacheKey) throws TTransportException {
    TTransport transport = ThriftUtil.createClientTransport(cacheKey.getServer(),
        (int) cacheKey.getTimeout(), cacheKey.getSslParams(), cacheKey.getSaslParams());

    log.trace("Creating new connection to connection to {}", cacheKey.getServer());

    CachedTTransport tsc = new CachedTTransport(transport, cacheKey);

    CachedConnection connection = new CachedConnection(tsc);
    connection.reserve();

    try {
      ConnectionPool pool = getConnectionPool();
      pool.putReserved(cacheKey, connection);
    } catch (TransportPoolShutdownException e) {
      connection.transport.close();
      throw e;
    }

    return connection.transport;
  }

  public void returnTransport(TTransport transport) {
    if (transport == null) {
      return;
    }

    CachedTTransport cachedTransport = (CachedTTransport) transport;
    ArrayList<CachedConnection> closeList = new ArrayList<>();
    ConnectionPool pool = getConnectionPool();
    boolean existInCache = pool.returnTransport(cachedTransport, closeList);

    // close outside of sync block
    closeList.forEach((connection) -> {
      try {
        connection.transport.close();
      } catch (Exception e) {
        log.debug("Failed to close connection w/ errors", e);
      }
    });

    if (cachedTransport.sawError) {

      boolean shouldWarn = false;
      Long ecount = null;

      synchronized (errorCount) {

        ecount = errorCount.merge(cachedTransport.getCacheKey(), 1L, Long::sum);

        // logs the first time an error occurred
        errorTime.computeIfAbsent(cachedTransport.getCacheKey(), k -> System.currentTimeMillis());

        if (ecount >= ERROR_THRESHOLD && serversWarnedAbout.add(cachedTransport.getCacheKey())) {
          // boolean facilitates logging outside of lock
          shouldWarn = true;
        }
      }

      log.trace("Returned connection had error {}", cachedTransport.getCacheKey());

      if (shouldWarn) {
        log.warn("Server {} had {} failures in a short time period, will not complain anymore",
            cachedTransport.getCacheKey(), ecount);
      }
    }

    if (!existInCache) {
      log.warn("Returned tablet server connection to cache that did not come from cache");
      // close outside of sync block
      transport.close();
    }
  }

  /**
   * Set the time after which idle connections should be closed
   */
  public synchronized void setIdleTime(long time) {
    this.killTime = time;
    log.debug("Set thrift transport pool idle time to {}", time);
  }

  private static ThriftTransportPool instance = null;

  static {
    SingletonManager.register(new SingletonService() {

      @Override
      public boolean isEnabled() {
        return ThriftTransportPool.isEnabled();
      }

      @Override
      public void enable() {
        ThriftTransportPool.enable();
      }

      @Override
      public void disable() {
        ThriftTransportPool.disable();
      }
    });
  }

  public static synchronized ThriftTransportPool getInstance() {
    Preconditions.checkState(instance != null,
        "The Accumulo singleton for connection pooling is disabled.  This is likely caused by all "
            + "AccumuloClients being closed or garbage collected.");
    instance.startCheckerThread();
    return instance;
  }

  private static synchronized boolean isEnabled() {
    return instance != null;
  }

  private static synchronized void enable() {
    if (instance == null) {
      // this code intentionally does not start the thread that closes idle connections. That thread
      // is created the first time something attempts to use this service.
      instance = new ThriftTransportPool();
    }
  }

  private static synchronized void disable() {
    if (instance != null) {
      try {
        instance.shutdown();
      } finally {
        instance = null;
      }
    }
  }

  public synchronized void startCheckerThread() {
    if (connectionPool != null && checkThread == null) {
      checkThread = new Daemon(new Closer(instance), "Thrift Connection Pool Checker");
      checkThread.start();
    }
  }

  void closeExpiredConnections() {
    List<CachedConnection> expiredConnections;

    synchronized (this) {
      ConnectionPool pool = getConnectionPool();
      expiredConnections = pool.removeExpiredConnections(killTime);

      synchronized (errorCount) {
        Iterator<Entry<ThriftTransportKey,Long>> iter = errorTime.entrySet().iterator();
        while (iter.hasNext()) {
          Entry<ThriftTransportKey,Long> entry = iter.next();
          long delta = System.currentTimeMillis() - entry.getValue();
          if (delta >= STUCK_THRESHOLD) {
            errorCount.remove(entry.getKey());
            iter.remove();
          }
        }
      }
    }

    // Close connections outside of sync block
    expiredConnections.forEach((c) -> c.transport.close());
  }

  private void shutdown() {
      Thread ctl;
      synchronized (this) {
        // Check if a shutdown has already occurred.
        if (this.connectionPool == null) {
          return;
        }

        // Shutdown the pool.
        connectionPool.shutdown();
        connectionPool = null;
        ctl = checkThread;
      }

      // Shut down the pool checker thread.
      if (ctl != null) {
        try {
          ctl.interrupt();
          ctl.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
  }

  private ConnectionPool getConnectionPool() {
    // Very important to only read volatile once into a local variable as multiple reads may see
    // different things.
    ConnectionPool local = connectionPool;
    if  (local == null) {
      throw new TransportPoolShutdownException(
          "The Accumulo singleton for connection pooling is disabled.  This is likely caused by "
              + "all AccumuloClients being closed or garbage collected.");
    }
    return local;
  }
}
