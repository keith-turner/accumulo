/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.google.common.base.Preconditions;

/**
 * Wraps another writer and does writes in another thread.
 */
public class ThreadedFileSKVWriter implements FileSKVWriter {

  private final FileSKVWriter wrappedWriter;

  private static final Map.Entry<Key,Value> END =
      new AbstractMap.SimpleImmutableEntry<>(new Key(), new Value());

  private WriteTask currentWriteTask;

  private class WriteTask implements Runnable {

    private final Thread thread;
    private final WeightedBlockingQueue<Map.Entry<Key,Value>> queue;

    private boolean stopped = false;
    private volatile Exception exception = null;

    @Override
    public void run() {
      try {
        ArrayList<Map.Entry<Key,Value>> entries = new ArrayList<>();
        while (true) {
          var e = queue.take();
          entries.add(e);
          queue.drainTo(entries);

          for (var entry : entries) {
            if (entry == END) {
              return;
            }
            wrappedWriter.append(entry.getKey(), entry.getValue());
          }
          entries.clear();
        }
      } catch (Exception e) {
        exception = e;
      }
    }

    WriteTask() {
      thread = new Thread(this);
      this.queue = new WeightedBlockingQueue<>(
          kve -> kve.getKey().getSize() + kve.getValue().getSize() + 64, 10_000_000);

    }

    void start() {
      thread.start();
    }

    void stop() {
      if (stopped) {
        return;
      }
      try {
        // TODO could get stuck forever if queue is full and background thread threw an exception
        queue.put(END);
        thread.join();
        stopped = true;

        if (exception != null) {
          throw new IllegalStateException(exception);
        }

      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    public void append(Key key, Value value) {
      Preconditions.checkState(!stopped);
      try {
        queue.put(new AbstractMap.SimpleImmutableEntry<>(key, value));
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public ThreadedFileSKVWriter(FileSKVWriter writer) {
    this.wrappedWriter = writer;

  }

  @Override
  public boolean supportsLocalityGroups() {
    return wrappedWriter.supportsLocalityGroups();
  }

  @Override
  public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies)
      throws IOException {
    if (currentWriteTask != null) {
      currentWriteTask.stop();
      currentWriteTask = null;
    }

    wrappedWriter.startNewLocalityGroup(name, columnFamilies);
    currentWriteTask = new WriteTask();
    currentWriteTask.start();
  }

  @Override
  public void startDefaultLocalityGroup() throws IOException {
    if (currentWriteTask != null) {
      currentWriteTask.stop();
      currentWriteTask = null;
    }

    wrappedWriter.startDefaultLocalityGroup();
    ;
    currentWriteTask = new WriteTask();
    currentWriteTask.start();
  }

  @Override
  public void append(Key key, Value value) throws IOException {
    currentWriteTask.append(key, value);
  }

  @Override
  public DataOutputStream createMetaStore(String name) throws IOException {
    if (currentWriteTask != null) {
      currentWriteTask.stop();
      currentWriteTask = null;
    }

    return wrappedWriter.createMetaStore(name);
  }

  @Override
  public void close() throws IOException {
    if (currentWriteTask != null) {
      currentWriteTask.stop();
      currentWriteTask = null;
    }

    wrappedWriter.close();
  }

  @Override
  public long getLength() throws IOException {
    Preconditions.checkState(currentWriteTask == null);
    return wrappedWriter.getLength();
  }
}
