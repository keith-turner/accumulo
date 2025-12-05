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
package org.apache.accumulo.core.trace;

public class ScanInstrumentation {

  // TODO this assumes a single thread will read and write
  private long fileBytesRead;
  private long uncompressedBytesRead;
  private int cacheHits;
  private int cacheMisses;

  // TODO pass this around instead of using a thread local?
  private static final ThreadLocal<ScanInstrumentation> INSTRUMENTED_THREADS = new ThreadLocal<>();

  public void addFileBytesRead(long amount) {
    fileBytesRead += amount;
  }

  public void addUncompressedBytesRead(long amount) {
    uncompressedBytesRead += amount;
  }

  public void addCacheMiss() {
    cacheMisses++;
  }

  public void addCacheHit() {
    cacheHits++;
  }

  public long getFileBytesRead() {
    return fileBytesRead;
  }

  public long getUncompressedBytesRead() {
    return uncompressedBytesRead;
  }

  public int getCacheHits() {
    return cacheHits;
  }

  public int getCacheMisses() {
    return cacheMisses;
  }

  public static void enable() {
    INSTRUMENTED_THREADS.set(new ScanInstrumentation());
  }

  public static ScanInstrumentation get() {
    return INSTRUMENTED_THREADS.get();
  }

  public static void disable() {
    INSTRUMENTED_THREADS.remove();
  }
}
