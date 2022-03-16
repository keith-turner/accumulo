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
package org.apache.accumulo.core.clientImpl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ScanServerDispatcher;
import org.apache.accumulo.core.spi.scan.ScanServerDispatcher.ScanAttempt;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

public class ScanAttemptsImpl {

  static class ScanAttemptImpl
      implements org.apache.accumulo.core.spi.scan.ScanServerDispatcher.ScanAttempt {

    private final String server;
    private final long time;
    private final Result result;
    private volatile long mutationCount = Long.MAX_VALUE;

    ScanAttemptImpl(Result result, String server, long time) {
      this.result = result;
      this.server = Objects.requireNonNull(server);
      this.time = time;
    }

    @Override
    public String getServer() {
      return server;
    }

    @Override
    public long getEndTime() {
      return time;
    }

    @Override
    public Result getResult() {
      return result;
    }

    private void setMutationCount(long mc) {
      this.mutationCount = mc;
    }

    public long getMutationCount() {
      return mutationCount;
    }

  }

  private Map<TabletId,Collection<ScanAttemptImpl>> attempts = new ConcurrentHashMap<>();
  private long mutationCounter = 0;

  private AtomicInteger currentIteration = new AtomicInteger(0);

  public void add(TabletId tablet, ScanAttempt.Result result, String server, long endTime) {

    ScanAttemptImpl sa = new ScanAttemptImpl(result, server, endTime);

    attempts.computeIfAbsent(tablet, k -> Collections.synchronizedList(new ArrayList<>())).add(sa);

    synchronized (this) {
      // now that the scan attempt obj is added to all concurrent data structs, make it visible

      // need to atomically increment the counter AND set the counter on the object
      sa.setMutationCount(mutationCounter++);
    }

  }

  public static interface ScanAttemptReporter {
    void report(ScanAttempt.Result result);
  }

  ScanAttemptReporter createReporter(String server, TabletId tablet) {
    var iteration = currentIteration.get();
    return new ScanAttemptReporter() {
      @Override
      public void report(ScanAttempt.Result result) {
        add(tablet, result, server, System.currentTimeMillis());
      }
    };
  }

  Map<TabletId,Collection<ScanAttemptImpl>> snapshot() {
    // allows only seeing scan attempt objs that were added before this call

    long snapMC;
    synchronized (ScanAttemptsImpl.this) {
      snapMC = mutationCounter;
    }
    return Maps.transformValues(attempts, tabletAttemptList -> Collections2
        .filter(tabletAttemptList, sai -> sai.getMutationCount() <= snapMC));
  }
}