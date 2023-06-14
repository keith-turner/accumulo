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
package org.apache.accumulo.server.manager.state;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.CHOPPED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACT_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.DIR;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_GOAL;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_REQUESTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SCANS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.server.metadata.TabletMutatorBase;
import org.apache.hadoop.io.Text;

public class TabletMetadataImposter extends TabletMetadata {

  // TODO replace existing usage of this with builder
  public TabletMetadataImposter(KeyExtent extent, Location future, Location current, Location last,
      SuspendingTServer suspend, List<LogEntry> walogs, boolean chopped, TabletHostingGoal goal,
      boolean onDemandHostingRequested) {
    super.extent = extent;
    super.endRow = extent.endRow();
    super.tableId = extent.tableId();
    if (current != null && future != null) {
      super.futureAndCurrentLocationSet = true;
      super.location = current;
    } else if (current != null) {
      super.location = current;
    } else if (future != null) {
      super.location = future;
    }
    super.last = last;
    super.suspend = suspend;
    super.logs = walogs == null ? new ArrayList<>() : walogs;
    super.chopped = chopped;
    super.goal = goal;
    super.onDemandHostingRequested = onDemandHostingRequested;
    super.fetchedCols = EnumSet.of(PREV_ROW, LOCATION, LAST, SUSPEND, LOGS, CHOPPED, HOSTING_GOAL,
        HOSTING_REQUESTED);
  }

  private static class InternalBuilder extends TabletMutatorBase<InternalBuilder> {
    protected InternalBuilder(KeyExtent extent) {
      super(null, extent); // TODO refactor out use of servercontext, only one method uses it so
                           // just pass that
    }

    public Mutation getMutation() {
      return super.getMutation();
    }
  }

  public static class Builder implements Ample.TabletUpdates<Builder> {

    private InternalBuilder internalBuilder;
    EnumSet<ColumnType> fetched;

    protected Builder(KeyExtent extent) {
      internalBuilder = new InternalBuilder(extent);
      fetched = EnumSet.noneOf(ColumnType.class);
      putPrevEndRow(extent.prevEndRow());
    }

    @Override
    public Builder putPrevEndRow(Text per) {
      fetched.add(PREV_ROW);
      internalBuilder.putPrevEndRow(per);
      return this;
    }

    @Override
    public Builder putFile(ReferencedTabletFile path, DataFileValue dfv) {
      fetched.add(FILES);
      internalBuilder.putFile(path, dfv);
      return this;
    }

    @Override
    public Builder putFile(StoredTabletFile path, DataFileValue dfv) {
      fetched.add(FILES);
      internalBuilder.putFile(path, dfv);
      return this;
    }

    @Override
    public Builder deleteFile(StoredTabletFile path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putScan(StoredTabletFile path) {
      fetched.add(SCANS);
      internalBuilder.putScan(path);
      return this;
    }

    @Override
    public Builder deleteScan(StoredTabletFile path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putCompactionId(long compactionId) {
      fetched.add(COMPACT_ID);
      internalBuilder.putCompactionId(compactionId);
      return this;
    }

    @Override
    public Builder putFlushId(long flushId) {
      fetched.add(FLUSH_ID);
      internalBuilder.putFlushId(flushId);
      return this;
    }

    @Override
    public Builder putLocation(Location location) {
      fetched.add(LOCATION);
      internalBuilder.putLocation(location);
      return this;
    }

    @Override
    public Builder deleteLocation(Location location) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putZooLock(ServiceLock zooLock) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putDirName(String dirName) {
      fetched.add(DIR);
      internalBuilder.putDirName(dirName);
      return this;
    }

    @Override
    public Builder putWal(LogEntry logEntry) {
      fetched.add(LOGS);
      internalBuilder.putWal(logEntry);
      return this;
    }

    @Override
    public Builder deleteWal(String wal) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder deleteWal(LogEntry logEntry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putTime(MetadataTime time) {
      fetched.add(TIME);
      internalBuilder.putTime(time);
      return this;
    }

    @Override
    public Builder putBulkFile(ReferencedTabletFile bulkref, long tid) {
      fetched.add(LOADED);
      internalBuilder.putBulkFile(bulkref, tid);
      return this;
    }

    @Override
    public Builder deleteBulkFile(ReferencedTabletFile bulkref) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putChopped() {
      fetched.add(CHOPPED);
      internalBuilder.putChopped();
      return this;
    }

    @Override
    public Builder putSuspension(TServerInstance tserver, long suspensionTime) {
      return null;
    }

    @Override
    public Builder deleteSuspension() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putExternalCompaction(ExternalCompactionId ecid,
        ExternalCompactionMetadata ecMeta) {
      fetched.add(ECOMP);
      internalBuilder.putExternalCompaction(ecid, ecMeta);
      return this;
    }

    @Override
    public Builder deleteExternalCompaction(ExternalCompactionId ecid) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putHostingGoal(TabletHostingGoal goal) {
      fetched.add(HOSTING_GOAL);
      internalBuilder.putHostingGoal(goal);
      return this;
    }

    @Override
    public Builder setHostingRequested() {
      fetched.add(HOSTING_REQUESTED);
      internalBuilder.setHostingRequested();
      return this;
    }

    @Override
    public Builder deleteHostingRequested() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putOperation(TabletOperationId opId) {
      fetched.add(OPID);
      internalBuilder.putOperation(opId);
      return this;
    }

    @Override
    public Builder deleteOperation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putSelectedFile(StoredTabletFile file, long tid) {
      fetched.add(SELECTED);
      internalBuilder.putSelectedFile(file, tid);
      return this;
    }

    @Override
    public Builder deleteSelectedFile(StoredTabletFile file) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder putCompacted(long tid) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Builder deleteCompacted(long tid) {
      throw new UnsupportedOperationException();
    }

    /**
     * @param extraFetched Anything that was put on the builder will automatically be added to the
     *        fetched set. However, for the case where something was not put and it needs to be
     *        fetched it can be passed here. For example to simulate a tablet w/o a location it, no
     *        location will be put and LOCATION would be passed in via this argument.
     * @return
     */
    public TabletMetadata build(ColumnType... extraFetched) {
      var mutation = internalBuilder.getMutation();

      SortedMap<Key,Value> rowMap = new TreeMap<>();
      mutation.getUpdates().forEach(cu -> {
        Key k = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
            cu.getTimestamp());
        Value v = new Value(cu.getValue());
        rowMap.put(k, v);
      });

      fetched.addAll(Arrays.asList(extraFetched));

      return TabletMetadata.convertRow(rowMap.entrySet().iterator(), fetched, true, false);
    }
  }

  // TODO maybe this could be moved to TabletMetadata
  public static Builder builder(KeyExtent extent) {
    return new Builder(extent);
  }

}
