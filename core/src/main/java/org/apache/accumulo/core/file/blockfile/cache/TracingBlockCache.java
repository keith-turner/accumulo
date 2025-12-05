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
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.Map;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.spi.cache.CacheType;
import org.apache.accumulo.core.trace.TraceUtil;

import com.google.common.collect.Maps;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

// TODO this is only tracking the amount of data loaded because of cache misses.  Could also track the amount of cached data a scan reads.
public class TracingBlockCache implements BlockCache {

  private final BlockCache blockCache;
  private final CacheType type;

  // TODO the opentelem javadocs strongly recommended creating these keys, should this be done
  // elsewhere in the code?
  private static final AttributeKey<String> BLOCK_NAME_KEY = AttributeKey.stringKey("block-name");
  private static final AttributeKey<String> CACHE_TYPE_KEY = AttributeKey.stringKey("cache-type");
  private static final AttributeKey<Long> BYTES_READ_KEY = AttributeKey.longKey("bytes-read");

  private TracingBlockCache(CacheType type, BlockCache blockCache) {
    this.type = type;
    this.blockCache = blockCache;
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return blockCache.cacheBlock(blockName, buf);
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    return blockCache.getBlock(blockName);
  }

  private final class TracingLoader implements Loader {
    private final Loader loader;
    private final String blockName;

    private TracingLoader(String blockName, Loader loader) {
      this.blockName = blockName;
      this.loader = loader;
    }

    @Override
    public Map<String,Loader> getDependencies() {
      var deps = loader.getDependencies();
      return Maps.transformEntries(deps, TracingLoader::new);
    }

    @Override
    public byte[] load(int maxSize, Map<String,byte[]> dependencies) {
      Span span = TraceUtil.startSpan(TracingLoader.class, "load-rfile-block");
      try (Scope scope = span.makeCurrent()) {
        byte[] data = loader.load(maxSize, dependencies);
        span.setAttribute(BLOCK_NAME_KEY, blockName);
        span.setAttribute(CACHE_TYPE_KEY, type.name());
        // TODO when null, other code will read the data outside the cache. Could instrument this
        // code also to track data.
        span.setAttribute(BYTES_READ_KEY, data == null ? -1 : data.length);
        return data;
      } finally {
        span.end();
      }
    }
  }

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    return blockCache.getBlock(blockName, new TracingLoader(blockName, loader));
  }

  @Override
  public long getMaxHeapSize() {
    return blockCache.getMaxHeapSize();
  }

  @Override
  public long getMaxSize() {
    return blockCache.getMaxSize();
  }

  @Override
  public Stats getStats() {
    return blockCache.getStats();
  }

  public static BlockCache wrap(CacheType type, BlockCache cache) {
    // TODO
    if (cache != null /* && TraceUtil.isTracingPossible() */) {
      return new TracingBlockCache(type, cache);
    } else {
      return cache;
    }
  }
}
