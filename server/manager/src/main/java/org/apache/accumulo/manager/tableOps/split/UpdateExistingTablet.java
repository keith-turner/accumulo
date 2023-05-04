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
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;

public class UpdateExistingTablet extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final KeyExtent expectedExtent;
  private final Text split;

  public UpdateExistingTablet(KeyExtent expectedExtent, Text split) {
    this.expectedExtent = expectedExtent;
    this.split = split;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      tabletsMutator.mutateTablet(expectedExtent).requireOperation(opid)
          .requirePrevEndRow(expectedExtent.prevEndRow()).putPrevEndRow(split).submit();

      var result = tabletsMutator.process().get(expectedExtent);

      if (result.getStatus() != ConditionalWriter.Status.ACCEPTED) {
        // maybe this step is being run again and the update was already made

        // look for the new tablet
        var newTabletMetadata = manager.getContext().getAmple()
            .readTablet(new KeyExtent(expectedExtent.tableId(), expectedExtent.endRow(), split));

        if (newTabletMetadata == null || newTabletMetadata.getOperationId() == null
            || !newTabletMetadata.getOperationId().equals(opid)) {
          throw new IllegalStateException(
              "Failed to update existing tablet in split " + expectedExtent + " " + split);
        }

      }
    }

    return new DeleteOperationIds(expectedExtent, split);
  }
}
