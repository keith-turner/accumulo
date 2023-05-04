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

import java.util.stream.Collectors;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;

public class DeleteOperationIds extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final KeyExtent originalExtent;
  private final Text split;

  public DeleteOperationIds(KeyExtent originalExtent, Text split) {
    this.originalExtent = originalExtent;
    this.split = split;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    KeyExtent lowExtent =
        new KeyExtent(originalExtent.tableId(), split, originalExtent.prevEndRow());
    KeyExtent highExtent = new KeyExtent(originalExtent.tableId(), originalExtent.endRow(), split);

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      // As long as the operation is not our operation id, then this step can be considered
      // successful in the case of unknown. If this repo is running for a second time and has
      // already deleted the operation id, then it could be absent or set by another fate operation.
      Ample.UnknownValidator unknownValidator =
          tabletMetadata -> tabletMetadata.getOperationId() == null
              || !tabletMetadata.getOperationId().equals(opid);

      tabletsMutator.mutateTablet(lowExtent).requireOperation(opid).deleteOperation()
          .submit(unknownValidator);
      tabletsMutator.mutateTablet(highExtent).requireOperation(opid).deleteOperation()
          .submit(unknownValidator);

      var results = tabletsMutator.process();
      boolean allAccepted = results.values().stream()
          .allMatch(result -> result.getStatus() == ConditionalWriter.Status.ACCEPTED);

      if (!allAccepted) {
        throw new IllegalStateException("Failed to delete operation ids " + originalExtent + " "
            + split + " " + results.values().stream().map(Ample.ConditionalResult::getStatus)
                .collect(Collectors.toSet()));
      }
    }

    return null;
  }
}
