/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.compactor;

import java.io.Closeable;
import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.ratelimit.SharedRateLimiterFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.Compactor.CompactionEnv;
import org.apache.accumulo.server.iterators.SystemIteratorEnvironment;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;

import com.google.common.annotations.VisibleForTesting;

public class CompactionEnvironment implements Closeable, CompactionEnv {

  private final ServerContext context;
  private final CompactionJobHolder jobHolder;
  private final SharedRateLimiterFactory limiter;
  private TExternalCompactionJob job;
  private String queueName;

  public static class CompactorIterEnv extends TabletIteratorEnvironment {

    private String queueName;

    public CompactorIterEnv(ServerContext context, IteratorScope scope, boolean fullMajC,
        AccumuloConfiguration tableConfig, TableId tableId, CompactionKind kind, String queueName) {
      super(context, scope, fullMajC, tableConfig, tableId, kind);
      this.queueName = queueName;
    }

    @VisibleForTesting
    public String getQueueName() {
      return queueName;
    }
  }

  CompactionEnvironment(ServerContext context, CompactionJobHolder jobHolder, String queueName) {
    this.context = context;
    this.jobHolder = jobHolder;
    this.job = jobHolder.getJob();
    this.limiter = SharedRateLimiterFactory.getInstance(this.context.getConfiguration());
    this.queueName = queueName;
  }

  @Override
  public void close() throws IOException {
    limiter.remove("read_rate_limiter");
    limiter.remove("write_rate_limiter");
  }

  @Override
  public boolean isCompactionEnabled() {
    return !jobHolder.isCancelled();
  }

  @Override
  public IteratorScope getIteratorScope() {
    return IteratorScope.majc;
  }

  @Override
  public RateLimiter getReadLimiter() {
    return limiter.create("read_rate_limiter", () -> job.getReadRate());
  }

  @Override
  public RateLimiter getWriteLimiter() {
    return limiter.create("write_rate_limiter", () -> job.getWriteRate());
  }

  @Override
  public SystemIteratorEnvironment createIteratorEnv(ServerContext context,
      AccumuloConfiguration acuTableConf, TableId tableId) {
    return new CompactorIterEnv(context, IteratorScope.majc,
        !jobHolder.getJob().isPropagateDeletes(), acuTableConf, tableId,
        CompactionKind.valueOf(job.getKind().name()), queueName);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> getMinCIterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TCompactionReason getReason() {
    switch (job.getKind()) {
      case USER:
        return TCompactionReason.USER;
      case CHOP:
        return TCompactionReason.CHOP;
      case SELECTOR:
      case SYSTEM:
      default:
        return TCompactionReason.SYSTEM;
    }
  }

}