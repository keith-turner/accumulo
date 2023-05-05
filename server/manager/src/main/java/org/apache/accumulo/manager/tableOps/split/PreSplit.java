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
package org.apache.accumulo.manager.tableOps.split;

import java.util.Map;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreSplit extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);

  private final SplitInfo splitInfo;

  public PreSplit(KeyExtent expectedExtent, Text split) {
    this.splitInfo = new SplitInfo(expectedExtent, split);
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {

    // TODO get tablet lock?
    // TODO check for offline table?

    TabletMetadata tabletMetadata = manager.getContext().getAmple().readTablet(
        splitInfo.getOriginal(), ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.OPID);

    if (tabletMetadata == null) {
      // TODO tablet does not exists, how to best handle?
      throw new NullPointerException();
    }

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    if (tabletMetadata.getLocation() != null) {
      // TODO ensure this removed
      log.debug("{} waiting for unassignment {}", FateTxId.formatTid(tid),
          tabletMetadata.getLocation());
      manager.requestUnassignment(splitInfo.getOriginal(), tid);
      return 1000;
    }

    if (tabletMetadata.getOperationId() != null) {
      if (tabletMetadata.getOperationId().equals(opid)) {
        log.trace("{} already set operation id", FateTxId.formatTid(tid));
        return 0;
      } else {
        log.debug("{} can not split, another operation is active {}", FateTxId.formatTid(tid),
            tabletMetadata.getOperationId());
        return 1000;
      }
    } else {
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireAbsentOperation()
            .requireAbsentLocation().requirePrevEndRow(splitInfo.getOriginal().prevEndRow())
            .putOperation(opid)
            .submit(tmeta -> tmeta.getOperationId() != null && tmeta.getOperationId().equals(opid));

        Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();

        if (results.get(splitInfo.getOriginal()).getStatus() == ConditionalWriter.Status.ACCEPTED) {
          // TODO change to trace
          log.debug("{} reserved {} for split", FateTxId.formatTid(tid), splitInfo.getOriginal());
          return 0;
        } else {
          tabletMetadata = results.get(splitInfo.getOriginal()).readMetadata();

          log.debug("{} Failed to set operation id. extent:{} location:{} opid:{}",
              FateTxId.formatTid(tid), splitInfo.getOriginal(), tabletMetadata.getLocation(),
              tabletMetadata.getOperationId());

          // TODO write IT that spins up 100 threads that all try to add a diff split to the same
          // tablet

          return 1000;
        }
      }
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    // ELASTICITY_TODO need to make manager ignore tablet with an operation id for assignment
    // purposes
    manager.cancelUnassignmentRequest(splitInfo.getOriginal(), tid);

    // Create the dir name here for the next step. If the next step fails it will always have the
    // same dir name each time it runs again making it idempotent.
    String dirName =
        UniqueNameAllocator.createTabletDirectoryName(manager.getContext(), splitInfo.getSplit());

    return new AddNewTablet(splitInfo, dirName);
  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    // TODO is this called if isReady fails?
    manager.cancelUnassignmentRequest(splitInfo.getOriginal(), tid);
  }
}
