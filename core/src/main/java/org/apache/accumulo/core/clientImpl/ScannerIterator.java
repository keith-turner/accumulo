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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ThriftScanner.ScanState;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.NamingThreadFactory;

import com.google.common.base.Preconditions;

public class ScannerIterator implements Iterator<Entry<Key,Value>> {

  // scanner options
  private long timeOut;

  // scanner state
  private Iterator<KeyValue> iter;
  private final ScanState scanState;

  private ScannerOptions options;

  private Future<List<KeyValue>> readAheadOperation;

  private boolean finished = false;

  private long batchCount = 0;
  private long readaheadThreshold;

  private Set<ScannerIterator> activeIters;

  private static ThreadPoolExecutor readaheadPool = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 3L,
      TimeUnit.SECONDS, new SynchronousQueue<>(),
      new NamingThreadFactory("Accumulo scanner read ahead thread"));

  private boolean closed = false;

  private synchronized List<KeyValue> readBatch() throws Exception {

    List<KeyValue> batch;

    do {
      synchronized (scanState) {
        // this is synchronized so its mutually exclusive with closing
        Preconditions.checkState(!closed, "Scanner was closed");
        batch = ThriftScanner.scan(scanState.context, scanState, timeOut);
      }
    } while (batch != null && batch.size() == 0);

    return batch == null ? Collections.emptyList() : batch;
  }

  ScannerIterator(ClientContext context, Table.ID tableId, Authorizations authorizations,
      Range range, int size, long timeOut, ScannerOptions options, boolean isolated,
      long readaheadThreshold, Set<ScannerIterator> activeIters) {
    this.timeOut = timeOut;
    this.readaheadThreshold = readaheadThreshold;

    this.options = new ScannerOptions(options);

    this.activeIters = activeIters;

    if (this.options.fetchedColumns.size() > 0) {
      range = range.bound(this.options.fetchedColumns.first(), this.options.fetchedColumns.last());
    }

    scanState = new ScanState(context, tableId, authorizations, new Range(range),
        options.fetchedColumns, size, options.serverSideIteratorList,
        options.serverSideIteratorOptions, isolated, readaheadThreshold,
        options.getSamplerConfiguration(), options.batchTimeOut, options.classLoaderContext,
        options.executionHints);

    // If we want to start readahead immediately, don't wait for hasNext to be called
    if (readaheadThreshold == 0L) {
      initiateReadAhead();
    }
    iter = null;
  }

  private void initiateReadAhead() {
    Preconditions.checkState(readAheadOperation == null);
    readAheadOperation = readaheadPool.submit(this::readBatch);
  }

  private List<KeyValue> getNextBatch() {

    List<KeyValue> nextBatch;

    try {
      if (readAheadOperation == null) {
        // no read ahead run, fetch the next batch right now
        nextBatch = readBatch();
      } else {
        nextBatch = readAheadOperation.get();
        readAheadOperation = null;
      }
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof IsolationException)
        throw new IsolationException(ee);
      if (ee.getCause() instanceof TableDeletedException) {
        TableDeletedException cause = (TableDeletedException) ee.getCause();
        throw new TableDeletedException(cause.getTableId(), cause);
      }
      if (ee.getCause() instanceof TableOfflineException)
        throw new TableOfflineException(ee);
      if (ee.getCause() instanceof SampleNotPresentException)
        throw new SampleNotPresentException(ee.getCause().getMessage(), ee);

      throw new RuntimeException(ee);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (!nextBatch.isEmpty()) {
      batchCount++;

      if (batchCount > readaheadThreshold) {
        // start a thread to read the next batch
        initiateReadAhead();
      }
    }

    return nextBatch;
  }

  @Override
  public boolean hasNext() {
    if (finished)
      return false;

    if (iter != null && iter.hasNext()) {
      return true;
    }

    iter = getNextBatch().iterator();
    if (!iter.hasNext()) {
      finished = true;
      activeIters.remove(this);
      return false;
    }

    return true;
  }

  @Override
  public Entry<Key,Value> next() {
    if (hasNext())
      return iter.next();
    throw new NoSuchElementException();
  }

  // just here to satisfy the interface
  // could make this actually delete things from the database
  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported in Scanner");
  }

  void close() {
    // run actual close operation in the background so this does not block.
    readaheadPool.execute(() -> {
      synchronized (scanState) {
        // this is synchronized so its mutually exclusive with readBatch()
        closed = true;
        ThriftScanner.close(scanState);
      }
    });
  }
}
