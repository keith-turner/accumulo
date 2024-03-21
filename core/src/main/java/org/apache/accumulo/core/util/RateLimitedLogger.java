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
package org.apache.accumulo.core.util;

import java.time.Duration;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Rate limits logging by an arbitrary key and does not fill the memory with old unused keys.
 */
public class RateLimitedLogger<K> {
  private final Cache<K,Long> loggedCache;
  private final Logger log;

  public RateLimitedLogger(Logger log, Duration limit) {
    this.log = log;
    this.loggedCache = CacheBuilder.newBuilder().expireAfterWrite(limit).build();
  }

  public void run(K key, Consumer<Logger> loggerConsumer) {
    var last = loggedCache.getIfPresent(key);

    if (last == null) {
      loggerConsumer.accept(log);
      loggedCache.put(key, System.currentTimeMillis());
    }
  }

}
