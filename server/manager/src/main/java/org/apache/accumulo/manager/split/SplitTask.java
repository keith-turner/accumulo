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
package org.apache.accumulo.manager.split;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.FileUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SplitTask.class);
  private final Manager manager;

  private final ServerContext context;
  private final Set<KeyExtent> queuedForSplit;
  private final TabletMetadata tablet;

  public SplitTask(ServerContext context, Set<KeyExtent> queuedForSplit, TabletMetadata tablet,
      Manager manager) {
    this.context = context;
    this.queuedForSplit = queuedForSplit;
    this.tablet = tablet;
    this.manager = manager;
  }

  @Override
  public void run() {
    try {
      System.out.println("running split task for " + tablet.getExtent());

      // TODO reread tablet metadata, may have been queued for a while and things could have changed

      var extent = tablet.getExtent();
      var files =
          tablet.getFiles().stream().map(TabletFile::getPathStr).collect(Collectors.toList());
      var tableConfiguration = context.getTableConfiguration(extent.tableId());

      SortedMap<Double,Key> keys = FileUtil.findMidPoint(context, tableConfiguration, null,
          extent.prevEndRow(), extent.endRow(), files, .25, true);

      // TODO code in tablet class does alot of checks that need to be done here.
      Text split = keys.get(.5).getRow();

      long fateTxId = manager.fate().startTransaction();

      // TODO does the name (first arg below) need to be an enum
      manager.fate().seedTransaction("SYS_SPLIT", fateTxId, new PreSplit(extent, split), true,
          "System initiated split of tablet " + extent + " at " + split);

      // We have seeded the transaction, go ahead request unassignment of tablet. The fate tx will
      // also do this, but there are two reasons to do it here. First the thread that looks for
      // splits will ignore any extents that have unassignment requested. Second this gets the ball
      // rolling on unassignment before the fate op runs for the first time. Only want to do this
      // after seeding the fate because that ensures the unassignment request will be canceled
      // later.
      manager.requestUnassignment(extent, fateTxId);

      log.info("splitting {} at {}", tablet.getExtent(), split);

    } catch (Exception e) {
      log.error("Failed to split {}", tablet.getExtent(), e);
    } finally {
      queuedForSplit.remove(tablet.getExtent());
    }
  }
}
