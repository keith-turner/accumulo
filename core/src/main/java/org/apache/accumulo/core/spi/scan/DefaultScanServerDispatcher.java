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
package org.apache.accumulo.core.spi.scan;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.data.TabletId;

import com.google.common.hash.Hashing;

public class DefaultScanServerDispatcher implements ScanServerDispatcher {

  private static final SecureRandom RANDOM = new SecureRandom();
  private static final long INITIAL_SLEEP_TIME = 100L;
  private static final long MAX_SLEEP_TIME = Duration.ofMinutes(30).toMillis();
  private final int INITIAL_SERVERS = 3;
  private final int MAX_DEPTH = 3;

  InitParameters init;
  Duration defaultBusyTimeout;

  @Override
  public void init(InitParameters params) {
    init = params;
    defaultBusyTimeout = Duration.of(33, ChronoUnit.MILLIS);
  }

  @Override
  public Actions determineActions(DispatcherParameters params) {

    List<String> orderedScanServers = new ArrayList<>(init.getScanServers().get());
    Collections.sort(orderedScanServers);

    if (orderedScanServers.isEmpty()) {
      return new Actions() {
        @Override public String getScanServer(TabletId tabletId) {
          return null;
        }

        @Override public Duration getDelay() {
          return Duration.ZERO;
        }

        @Override public Duration getBusyTimeout() {
          return Duration.ZERO;
        }
      };
    }

   Map<TabletId, String> serversToUse = new HashMap<>();

    long maxBusyAttempts = 0;

    for (TabletId tablet : params.getTablets()) {

      // TODO handle io errors
      long busyAttempts = params.getAttempts(tablet).stream().filter(sa -> sa.getResult() == ScanAttempt.Result.BUSY).count();

      maxBusyAttempts = Math.max(maxBusyAttempts, busyAttempts);

      String serverToUse = null;

        int hashCode = hashTablet(tablet);

        int numServers;

        if (busyAttempts < MAX_DEPTH) {
          numServers = (int) Math.round(
              INITIAL_SERVERS * Math.pow(orderedScanServers.size() / (double) INITIAL_SERVERS,
                  busyAttempts / (double) MAX_DEPTH));
        } else {
          numServers = orderedScanServers.size();
        }

        int serverIndex =
            Math.abs(hashCode + RANDOM.nextInt(numServers)) % orderedScanServers.size();

        // TODO could check if errors were seen on this server in past attempts
        serverToUse = orderedScanServers.get(serverIndex);

      serversToUse.put(tablet, serverToUse);
    }

    //TODO make configurable
    long busyTimeout = 33L;

    if (maxBusyAttempts > MAX_DEPTH) {
      busyTimeout = (long) (INITIAL_SLEEP_TIME * Math.pow(2, maxBusyAttempts - (MAX_DEPTH + 1)));
      busyTimeout = Math.min(busyTimeout, MAX_SLEEP_TIME);
    }

    Duration busyTO = Duration.ofMillis(busyTimeout);

    return new Actions() {
      @Override public String getScanServer(TabletId tabletId) {
        return serversToUse.get(tabletId);
      }

      @Override public Duration getDelay() {
        return Duration.ZERO;
      }

      @Override public Duration getBusyTimeout() {
        return busyTO;
      }
    };
  }

  private int hashTablet(TabletId tablet) {
    var hasher = Hashing.murmur3_32_fixed().newHasher();

    hasher.putString(tablet.getTable().canonical(), UTF_8);

    if (tablet.getEndRow() != null) {
      hasher.putBytes(tablet.getEndRow().getBytes(), 0, tablet.getEndRow().getLength());
    }

    if (tablet.getPrevEndRow() != null) {
      hasher.putBytes(tablet.getPrevEndRow().getBytes(), 0, tablet.getPrevEndRow().getLength());
    }

    return hasher.hash().asInt();
  }
}
