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

import org.apache.accumulo.core.spi.cache.CacheType;

/**
 * This class helps collect per scan information for the purposes of tracing.
 */
public class ScanInstrumentation {

  // TODO this assumes a single thread will read and write
  private long fileBytesRead;
  private long uncompressedBytesRead;
  private final int[] cacheHits = new int[CacheType.values().length];
  private final int[] cacheMisses = new int[CacheType.values().length];
  private final int[] cacheBypasses = new int[CacheType.values().length];

  // TODO pass this around instead of using a thread local?
  private static final ThreadLocal<ScanInstrumentation> INSTRUMENTED_THREADS = new ThreadLocal<>();

  /**
   * Increments the raw bytes read directly from DFS by a scan.
   *
   * @param amount the amount of bytes read
   */
  public void incrementFileBytesRead(long amount) {
    fileBytesRead += amount;
  }

  // TODO should it be an option to cache compressed data?
  /**
   * Increments the uncompressed and decrypted bytes read by a scan. This will include all
   * uncompressed data read by a scan regardless of if the underlying data came from cache or DFS.
   *
   * @param amount
   */
  public void incrementUncompressedBytesRead(long amount) {
    uncompressedBytesRead += amount;
  }

  /**
   * Increments the count of rfile blocks that were not already in the cache.
   */
  public void incrementCacheMiss(CacheType cacheType) {
    cacheMisses[cacheType.ordinal()]++;
  }

  /**
   * Increments the count of rfile blocks that were already in the cache.
   */
  public void incrementCacheHit(CacheType cacheType) {
    cacheHits[cacheType.ordinal()]++;
  }

  /**
   * Increments the count of rfile blocks that were directly read from DFS bypassing the cache.
   */
  public void incrementCacheBypass(CacheType cacheType) {
    cacheBypasses[cacheType.ordinal()]++;
  }

  public long getFileBytesRead() {
    return fileBytesRead;
  }

  public long getUncompressedBytesRead() {
    return uncompressedBytesRead;
  }

  public int getCacheHits(CacheType cacheType) {
    return cacheHits[cacheType.ordinal()];
  }

  public int getCacheMisses(CacheType cacheType) {
    return cacheMisses[cacheType.ordinal()];
  }

  public int getCacheBypasses(CacheType cacheType) {
    return cacheBypasses[cacheType.ordinal()];
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
