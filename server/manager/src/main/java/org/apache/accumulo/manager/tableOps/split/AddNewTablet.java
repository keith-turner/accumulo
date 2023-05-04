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
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

public class AddNewTablet extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final KeyExtent expectedExtent;
  private final Text split;
  private final String dirName;

  public AddNewTablet(KeyExtent expectedExtent, Text split, String dirName) {
    this.expectedExtent = expectedExtent;
    this.split = split;
    this.dirName = dirName;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    TabletMetadata tabletMetadata = manager.getContext().getAmple().readTablet(expectedExtent);

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);
    Preconditions
        .checkState(tabletMetadata != null && tabletMetadata.getOperationId().equals(opid));

    KeyExtent newExtent =
        new KeyExtent(expectedExtent.tableId(), split, expectedExtent.prevEndRow());

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      var mutator = tabletsMutator.mutateTablet(newExtent).requireAbsentTablet();

      mutator.putOperation(opid);
      mutator.putDirName(dirName);
      mutator.putTime(tabletMetadata.getTime());
      tabletMetadata.getFlushId().ifPresent(mutator::putFlushId);
      mutator.putPrevEndRow(newExtent.prevEndRow());
      tabletMetadata.getCompactId().ifPresent(mutator::putCompactionId);
      mutator.putHostingGoal(tabletMetadata.getHostingGoal());

      // TODO suspend??

      tabletMetadata.getLoaded().forEach(mutator::putBulkFile);
      tabletMetadata.getLogs().forEach(mutator::putWal);

      // TODO determine which files go to where
      tabletMetadata.getFilesMap().forEach((f, v) -> {
        // TODO use floats?
        mutator.putFile(f, new DataFileValue(v.getSize() / 2, v.getNumEntries() / 2, v.getTime()));
      });

      mutator.submit(afterMeta -> afterMeta.getOperationId() != null
          && afterMeta.getOperationId().equals(opid));

      // not expecting this to fail
      // TODO message with context if it does
      Preconditions.checkState(tabletsMutator.process().get(expectedExtent).getStatus()
          == ConditionalWriter.Status.ACCEPTED);
    }

    return new UpdateExistingTablet(expectedExtent, split);
  }
}
