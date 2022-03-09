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

import java.time.Duration;
import java.util.*;

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

import com.google.common.base.Preconditions;

/**
 * @since 2.1.0
 */
// this plugin decides how to handle eventually consistent scans on the client side... long name
// could be EventuallyConsistentScan(Manager/Dispatcher/Govenor).. shorter name
// EcScan(Manager/Dispatcher/Govenor)
public interface ScanServerDispatcher {

  public interface InitParameters {
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();

    /**
     * @return the set of live ScanServers. Whenever the set changes a new ScanServerDispatcher
     *         object will be created an initialized.
     */
    Set<String> getScanServers();
  }

  /**
   * This method is called once after a ScanDispatcher is instantiated.
   */
  default void init(InitParameters params) {
    Preconditions.checkArgument(params.getOptions().isEmpty(), "No options expected");
  }

  // this object is used to communicate what the previous actions were attempted, when they were
  // attempted, and the result of the attempt
  interface ScanAttempt extends Comparable<ScanAttempt> {

    // represents reasons that previous attempts to scan failed
    enum Result {
      BUSY, IO_ERROR, ERROR, SUCCESS
    }

    long getTime();

    Result getResult();

    ScanServerDispatcher.Action getAction();
  }

  public interface ScanAttempts {
    Collection<ScanAttempt> all();

    SortedSet<ScanAttempt> forServer(String server);

    SortedSet<ScanAttempt> forTablet(TabletId tablet);
  }

  public interface DispatcherParameters {

    /**
     * @return the set of tablets to be scanned
     */
    Collection<TabletId> getTablets();

    /**
     * @return scan attempt information (TODO: how is this used?)
     */
    ScanAttempts getScanAttempts();
  }

  public static abstract class Action {

    private final String server;
    private final Collection<TabletId> tablets;

    protected Action(String server, Collection<TabletId> tablets) {
      Preconditions.checkArgument(tablets != null && !tablets.isEmpty());
      this.server = Objects.requireNonNull(server);
      this.tablets = tablets;
    }

    public String getServer() {
      return server;
    }

    public Collection<TabletId> getTablets() {
      return tablets;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      Action action = (Action) o;
      return server.equals(action.server) && tablets.equals(action.tablets);
    }

    @Override
    public int hashCode() {
      return Objects.hash(server, tablets);
    }
  }

  public static class UseScanServerAction extends Action {

    private final Duration delay;
    private final Duration busyTimeout;

    public UseScanServerAction(String server, Collection<TabletId> tablets, Duration delay,
        Duration busyTimeout) {
      super(server, tablets);
      this.delay = delay;
      this.busyTimeout = busyTimeout;
    }

    public Duration getDelay() {
      return delay;
    }

    public Duration getBusyTimeout() {
      return busyTimeout;
    }
  }

  public static class UseTserverAction extends Action {
    public UseTserverAction(String server, Collection<TabletId> tablets) {
      super(server, tablets);
    }
  }

  // TODO need a better name.. this interface is used to communicate what actions the plugin would
  // like Accumulo to take for the scan... maybe EcScanActions
  public interface Actions extends Iterable<Action> {

    public Optional<Action> getAction(TabletId tablet);

    public static Actions from(Collection<Action> actions) {
      return new Actions() {
        @Override
        public Iterator<Action> iterator() {
          return actions.iterator();
        }

        @Override
        public Optional<Action> getAction(TabletId tablet) {
          for (Action action : actions) {
            if (action.getTablets().contains(tablet)) {
              return Optional.of(action);
            }
          }

          return Optional.empty();
        }
      };
    }

  }

  /**
   * Uses the DispatcherParameters to determine which, if any, ScanServer should be used for
   * scanning a tablet.
   *
   * @param params
   *          parameters for the calculation
   * @return results
   */
  Actions determineActions(DispatcherParameters params);
}
