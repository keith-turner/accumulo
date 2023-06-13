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
package org.apache.accumulo.manager.tableOps.compact;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACT_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.manager.tableOps.delete.PreDeleteTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CompactionDriver extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(CompactionDriver.class);

  public static String createCompactionCancellationPath(InstanceId instanceId, TableId tableId) {
    return Constants.ZROOT + "/" + instanceId + Constants.ZTABLES + "/" + tableId.canonical()
        + Constants.ZTABLE_COMPACT_CANCEL_ID;
  }

  private static final long serialVersionUID = 1L;

  private long compactId;
  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;

  public CompactionDriver(long compactId, NamespaceId namespaceId, TableId tableId, byte[] startRow,
      byte[] endRow) {
    this.compactId = compactId;
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {

    if (tableId.equals(RootTable.ID)) {
      // this codes not properly handle the root table. See #798
      return 0;
    }

    String zCancelID = createCompactionCancellationPath(manager.getInstanceID(), tableId);
    ZooReaderWriter zoo = manager.getContext().getZooReaderWriter();

    if (Long.parseLong(new String(zoo.getData(zCancelID))) >= compactId) {
      // compaction was canceled
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OTHER,
          TableOperationsImpl.COMPACTION_CANCELED_MSG);
    }

    String deleteMarkerPath =
        PreDeleteTable.createDeleteMarkerPath(manager.getInstanceID(), tableId);
    if (zoo.exists(deleteMarkerPath)) {
      // table is being deleted
      throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT, TableOperationExceptionType.OTHER,
          TableOperationsImpl.TABLE_DELETED_MSG);
    }

    long t1 = System.currentTimeMillis();

    int tabletsToWaitFor = updateAndCheckTablets(manager, tid, compactId);

    long scanTime = System.currentTimeMillis() - t1;

    if (tabletsToWaitFor == 0) {
      return 0;
    }

    long sleepTime = 500;

    sleepTime = Math.max(2 * scanTime, sleepTime);

    sleepTime = Math.min(sleepTime, 30000);

    return sleepTime;
  }

  public int updateAndCheckTablets(Manager manager, long tid, long compactId) {
    // TODO select columns

    var ample = manager.getContext().getAmple();

    try (
        var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
            .fetch(PREV_ROW, COMPACT_ID, FILES, SELECTED, ECOMP, OPID).build();
        var tabletsMutator = ample.conditionallyMutateTablets()) {

      int complete = 0;
      int total = 0;

      for (TabletMetadata tablet : tablets) {

        total++;

        // TODO change all logging to trace

        if (tablet.getCompactId().orElse(-1) >= compactId) {
          // this tablet is already considered done
          log.debug("{} compaction for {} is complete", FateTxId.formatTid(tid),
              tablet.getExtent());
          complete++;
        } else if (tablet.getOperationId() != null) {
          log.debug("{} ignoring tablet {} with active operation {} ", FateTxId.formatTid(tid),
              tablet.getExtent(), tablet.getOperationId());
        } else if (tablet.getFiles().isEmpty()) {
          log.debug("{} tablet {} has no files, attempting to mark as compacted ",
              FateTxId.formatTid(tid), tablet.getExtent());
          // this tablet has no files try to mark it as done
          // TODO this could lower the existing compaction id
          tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requirePrevEndRow(tablet.getPrevEndRow()).requireAbsentFiles()
              .putCompactionId(compactId)
              .submit(tabletMetadata -> tabletMetadata.getCompacted().contains(tid));
        } else if (tablet.getSelectedFiles().isEmpty()
            && tablet.getExternalCompactions().isEmpty()) {
          // there are no selected files

          log.debug("{} selecting {} files compaction for {}", FateTxId.formatTid(tid),
              tablet.getFiles().size(), tablet.getExtent());

          // TODO need to call files selector if configured

          var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requirePrevEndRow(tablet.getPrevEndRow());
          tablet.getFiles().forEach(mutator::requireFile);
          // TODO require absent selected files

          mutator.requireAbsentSelectedFiles();
          mutator.requireAbsentCompactions();

          tablet.getFiles().forEach(file -> {
            mutator.requireFile(file);
            mutator.putSelectedFile(file, tid);
          });

          mutator.submit(tabletMetadata -> tabletMetadata.getSelectedFiles().keySet()
              .equals(tabletMetadata.getFiles())
              && tabletMetadata.getSelectedFiles().values().stream()
                  .allMatch(sftid -> sftid == tid));

        } else {
          // unable to select files at this time OR maybe we already selected and now have to wait
          // for compacted marker
          // TODO add selecting marker that prevents compactions
        }
      }

      tabletsMutator.process().values().stream()
          .filter(result -> result.getStatus() == Status.REJECTED)
          .forEach(result -> log.debug("{} update for {} was rejected ", FateTxId.formatTid(tid),
              result.getExtent()));

      // TODO should notify TGW if files were selected

      return total - complete;
    }

    // TODO need to handle total == 0
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    CompactRange.removeIterators(env, tid, tableId);
    Utils.getReadLock(env, tableId, tid).unlock();
    Utils.getReadLock(env, namespaceId, tid).unlock();
    return null;
  }

  @Override
  public void undo(long tid, Manager environment) {

  }

}
