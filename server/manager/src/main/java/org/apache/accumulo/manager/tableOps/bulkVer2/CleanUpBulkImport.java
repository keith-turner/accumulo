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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUpBulkImport extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  private BulkInfo info;

  public CleanUpBulkImport(BulkInfo info) {
    this.info = info;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    manager.updateBulkImportStatus(info.sourceDir, BulkImportState.CLEANUP);
    log.debug("removing the bulkDir processing flag file in " + info.bulkDir);
    Ample ample = manager.getContext().getAmple();
    Path bulkDir = new Path(info.bulkDir);
    ample.removeBulkLoadInProgressFlag(
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    ample.putGcFileAndDirCandidates(info.tableId,
        Collections.singleton(new ReferenceFile(info.tableId, bulkDir.toString())));

    log.debug("removing the metadata table markers for loaded files");
    removeBulkLoadEntries(ample, info.tableId, tid);

    Utils.unreserveHdfsDirectory(manager, info.sourceDir, tid);
    Utils.getReadLock(manager, info.tableId, tid).unlock();
    // delete json renames and mapping files
    Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Path mappingFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    try {
      manager.getVolumeManager().delete(renamingFile);
      manager.getVolumeManager().delete(mappingFile);
    } catch (IOException ioe) {
      log.debug("Failed to delete renames and/or loadmap", ioe);
    }

    log.debug("completing bulkDir import transaction " + FateTxId.formatTid(tid));
    manager.removeBulkImportStatus(info.sourceDir);
    return null;
  }

  private static void removeBulkLoadEntries(Ample ample, TableId tableId, long tid) {

    Retry retry = Retry.builder().infiniteRetries().retryAfter(100, MILLISECONDS)
        .incrementBy(100, MILLISECONDS).maxWait(1, SECONDS).backOffFactor(1.5)
        .logInterval(3, MINUTES).createRetry();

    while (true) {
      try (
          var tablets = ample.readTablets().forTable(tableId).checkConsistency()
              .fetch(ColumnType.PREV_ROW, ColumnType.LOADED).build();
          var tabletsMutator = ample.conditionallyMutateTablets()) {

        for (var tablet : tablets) {
          if (tablet.getLoaded().values().stream().anyMatch(l -> l == tid)) {
            var tabletMutator =
                tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation();
            tablet.getLoaded().entrySet().stream().filter(entry -> entry.getValue() == tid)
                .map(Map.Entry::getKey).forEach(tabletMutator::deleteBulkFile);
            tabletMutator.submit();
          }
        }

        var results = tabletsMutator.process();

        if (results.values().stream()
            .anyMatch(condResult -> condResult.getStatus() != Status.ACCEPTED)) {

          results.forEach((extent, condResult) -> {
            if (condResult.getStatus() != Status.ACCEPTED) {
              var metadata = condResult.readMetadata();
              log.debug("Tablet update failed {} {} {} {} ", FateTxId.formatTid(tid), extent,
                  condResult.getStatus(), metadata.getOperationId());
            }
          });

          try {
            retry.waitForNextAttempt(log,
                String.format("%s tableId:%s conditional mutations to delete load markers failed.",
                    FateTxId.formatTid(tid), tableId));
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        } else {
          break;
        }
      }
    }
  }
}
