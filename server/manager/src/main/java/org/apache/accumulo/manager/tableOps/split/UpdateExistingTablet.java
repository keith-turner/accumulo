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

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;

public class UpdateExistingTablet extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final SplitInfo splitInfo;

  public UpdateExistingTablet(SplitInfo splitInfo) {
    this.splitInfo = splitInfo;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    // Its important that if the repos fail and run again that we do not repeatedly half the files
    // sizes. They way things are currently done across multiple repos that should be avoided.
    // However, its some to consider for future changes.

    var originalTabletMetadata =
        manager.getContext().getAmple().readTablet(splitInfo.getOriginal());

    if (originalTabletMetadata == null) {
      // maybe this is step is running again and the new tablet exists
      var newTabletMetadata = manager.getContext().getAmple().readTablet(splitInfo.getHigh());

      if (newTabletMetadata == null || newTabletMetadata.getOperationId() == null
          || !newTabletMetadata.getOperationId().equals(opid)) {
        throw new IllegalStateException("Failed to update existing tablet in split "
            + splitInfo.getOriginal() + " " + splitInfo.getSplit());
      }

    } else {
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

        var mutator = tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireOperation(opid)
            .requirePrevEndRow(splitInfo.getOriginal().prevEndRow());

        mutator.putPrevEndRow(splitInfo.getSplit());

        // update existing tablets file sizes
        originalTabletMetadata.getFilesMap().forEach((f, v) -> {
          // TODO use split percentage
          mutator.putFile(f, new DataFileValue(v.getSize() - (v.getSize() / 2),
              v.getNumEntries() - (v.getNumEntries() / 2), v.getTime()));
        });

        mutator.submit();

        var result = tabletsMutator.process().get(splitInfo.getOriginal());

        if (result.getStatus() != ConditionalWriter.Status.ACCEPTED) {
          // maybe this step is being run again and the update was already made
          throw new IllegalStateException("Failed to update existing tablet in split "
              + splitInfo.getOriginal() + " " + splitInfo.getSplit() + " " + result.getStatus());

        }
      }
    }
    return new DeleteOperationIds(splitInfo);
  }
}
