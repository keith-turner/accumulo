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

import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;

public class Scratch {
  public void scratch(Ample ample, long tid) {
    try (var tablets = ample.readTablets().forTable(null).build();
        var tabletsMutator = ample.conditionallyMutateTablets()) {
      boolean allCompacted = true;

      for (TabletMetadata tablet : tablets) {

        // TODO need to handle the case where tablet has no files

        boolean alreadyCompacted = tablet.getCompacted().contains(tid);

        allCompacted &= alreadyCompacted;

        if (!alreadyCompacted && tablet.getSelectedFiles().isEmpty()
            && tablet.getExternalCompactions().isEmpty()) {
          // need to select files for the tablet
          // TODO need to require no selected files
          var mutator = tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requirePrevEndRow(tablet.getPrevEndRow()).requireAbsentCompactions();
          tablet.getFiles().forEach(file -> mutator.putSelectedFile(file, tid));

          mutator.submit(tm -> tm.getSelectedFiles().keySet().equals(tablet.getFiles())
              && tm.getSelectedFiles().values().stream().allMatch(sfid -> sfid == tid));

        }

        // TODO if there are external compactions need write a marker that prevents future external
        // compactions from starting

        // TODO in the case where the tablet has no files, may want to write the compacted marker
      }

      tabletsMutator.process();

    }

  }

}
