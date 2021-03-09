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
package org.apache.accumulo.coordinator;

import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorLockWatcher implements ZooLock.AccumuloLockWatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLockWatcher.class);

  @Override
  public void lostLock(LockLossReason reason) {
    Halt.halt("Coordinator lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
  }

  @Override
  public void unableToMonitorLockNode(final Exception e) {
    // ACCUMULO-3651 Changed level to error and added FATAL to message for slf4j compatibility
    Halt.halt(-1, () -> LOG.error("FATAL: No longer able to monitor Coordinator lock node", e));

  }

  @Override
  public synchronized void acquiredLock() {
    // This is overridden by the LockWatcherWrapper in ZooLock.tryLock()
  }

  @Override
  public synchronized void failedToAcquireLock(Exception e) {
    // This is overridden by the LockWatcherWrapper in ZooLock.tryLock()
  }

}
