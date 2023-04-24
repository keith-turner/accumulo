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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.clientImpl.bulk.Bulk;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletingest.thrift.TabletIngestClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * Make asynchronous load calls to each overlapping Tablet. This RepO does its work on the isReady
 * and will return a linear sleep value based on the largest number of Tablets on a TabletServer.
 */
class LoadFiles extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(LoadFiles.class);

  private final BulkInfo bulkInfo;

  public LoadFiles(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {
    if (manager.onlineTabletServers().isEmpty()) {
      log.warn("There are no tablet server to process bulkDir import, waiting (tid = "
          + FateTxId.formatTid(tid) + ")");
      return 100;
    }
    VolumeManager fs = manager.getVolumeManager();
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    manager.updateBulkImportStatus(bulkInfo.sourceDir, BulkImportState.LOADING);
    try (LoadMappingIterator lmi =
        BulkSerialize.getUpdatedLoadMapping(bulkDir.toString(), bulkInfo.tableId, fs::open)) {
      return loadFiles(bulkInfo.tableId, bulkDir, lmi, manager, tid);
    }
  }

  @Override
  public Repo<Manager> call(final long tid, final Manager manager) {
    if (bulkInfo.tableState == TableState.ONLINE) {
      return new RefreshTablets(bulkInfo);
    } else {
      return new CleanUpBulkImport(bulkInfo);
    }
  }

  private abstract static class Loader {
    protected Path bulkDir;
    protected Manager manager;
    protected long tid;
    protected boolean setTime;

    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      this.bulkDir = bulkDir;
      this.manager = manager;
      this.tid = tid;
      this.setTime = setTime;
    }

    abstract void load(List<TabletMetadata> tablets, Files files) throws Exception;

    abstract long finish() throws Exception;
  }

  // TODO name
  private static class OndemandLoader extends Loader {

    Ample.ConditionalTabletsMutator conditionalMutator;

    @Override
    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      Preconditions.checkArgument(!setTime);
      super.start(bulkDir, manager, tid, setTime);
      conditionalMutator = manager.getContext().getAmple().conditionallyMutateTablets();
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) {
      byte[] fam = TextUtil.getBytes(DataFileColumnFamily.NAME);

      for (TabletMetadata tablet : tablets) {
        Map<TabletFile,DataFileValue> filesToLoad = new HashMap<>();

        for (final Bulk.FileInfo fileInfo : files) {
          filesToLoad.put(new TabletFile(new Path(bulkDir, fileInfo.getFileName())),
              new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries()));
        }

        // remove any files that were already loaded
        filesToLoad.keySet().removeAll(tablet.getLoaded().keySet());

        if (!filesToLoad.isEmpty()) {
          // TODO require that files to load are absent
          // TODO lets always call require prev end row
          var tabletMutator =
              conditionalMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation();

          filesToLoad.forEach((f, v) -> {
            tabletMutator.putBulkFile(f, tid);
            tabletMutator.putFile(f, v);
          });

          if (tablet.getLocation() != null) {
            // only set the refresh if the location is still set, otherwise lets delete it
            tabletMutator.requireLocation(tablet.getLocation());
            tabletMutator.putRefreshId(tid);
          } else {
            // ensure tablet does not concurrently load while we are adding files, if it did would
            // need to ask it to refresh
            tabletMutator.requireAbsentLocation();
          }

          tabletMutator.submit();
        }
      }
    }

    @Override
    long finish() {
      var results = conditionalMutator.process();

      boolean allDone = results.values().stream()
          .allMatch(result -> result.getStatus() == ConditionalWriter.Status.ACCEPTED);

      long sleepTime = 0;
      if (!allDone) {
        // TODO compute sleep time based on characteristics of number of failures
        sleepTime = 1000;
      }

      return sleepTime;
    }
  }

  // TODO remove
  private static class OfflineLoader extends Loader {

    BatchWriter bw;

    // track how many tablets were sent load messages per tablet server
    MapCounter<HostAndPort> unloadingTablets;

    @Override
    void start(Path bulkDir, Manager manager, long tid, boolean setTime) throws Exception {
      Preconditions.checkArgument(!setTime);
      super.start(bulkDir, manager, tid, setTime);
      bw = manager.getContext().createBatchWriter(MetadataTable.NAME);
      unloadingTablets = new MapCounter<>();
    }

    @Override
    void load(List<TabletMetadata> tablets, Files files) throws MutationsRejectedException {
      byte[] fam = TextUtil.getBytes(DataFileColumnFamily.NAME);

      for (TabletMetadata tablet : tablets) {
        if (tablet.getLocation() != null) {
          unloadingTablets.increment(tablet.getLocation().getHostAndPort(), 1L);
          continue;
        }

        Mutation mutation = new Mutation(tablet.getExtent().toMetaRow());

        for (final Bulk.FileInfo fileInfo : files) {
          String fullPath = new Path(bulkDir, fileInfo.getFileName()).toString();
          byte[] val =
              new DataFileValue(fileInfo.getEstFileSize(), fileInfo.getEstNumEntries()).encode();
          mutation.put(fam, fullPath.getBytes(UTF_8), val);
        }

        bw.addMutation(mutation);
      }
    }

    @Override
    long finish() throws Exception {

      bw.close();

      long sleepTime = 0;
      if (unloadingTablets.size() > 0) {
        // find which tablet server had the most tablets to unload and sleep 13ms for each tablet
        sleepTime = unloadingTablets.max() * 13;
      }

      return sleepTime;
    }
  }

  /**
   * Make asynchronous load calls to each overlapping Tablet in the bulk mapping. Return a sleep
   * time to isReady based on a factor of the TabletServer with the most Tablets. This method will
   * scan the metadata table getting Tablet range and location information. It will return 0 when
   * all files have been loaded.
   */
  private long loadFiles(TableId tableId, Path bulkDir, LoadMappingIterator loadMapIter,
      Manager manager, long tid) throws Exception {
    PeekingIterator<Map.Entry<KeyExtent,Bulk.Files>> lmi = new PeekingIterator<>(loadMapIter);
    Map.Entry<KeyExtent,Bulk.Files> loadMapEntry = lmi.peek();

    Text startRow = loadMapEntry.getKey().prevEndRow();

    Iterator<TabletMetadata> tabletIter =
        TabletsMetadata.builder(manager.getContext()).forTable(tableId).overlapping(startRow, null)
            .checkConsistency().fetch(PREV_ROW, LOCATION, LOADED).build().iterator();

    Loader loader;
    if (bulkInfo.tableState == TableState.ONLINE) {
      loader = new OndemandLoader();
    } else {
      // TODO
      loader = new OfflineLoader();
    }

    loader.start(bulkDir, manager, tid, bulkInfo.setTime);

    long t1 = System.currentTimeMillis();
    while (lmi.hasNext()) {
      loadMapEntry = lmi.next();
      List<TabletMetadata> tablets = findOverlappingTablets(loadMapEntry.getKey(), tabletIter);
      loader.load(tablets, loadMapEntry.getValue());
    }

    long sleepTime = loader.finish();
    if (sleepTime > 0) {
      long scanTime = Math.min(System.currentTimeMillis() - t1, 30000);
      sleepTime = Math.max(sleepTime, scanTime * 2);
    }
    return sleepTime;
  }

  private static final Comparator<Text> PREV_COMP = Comparator.nullsFirst(Text::compareTo);
  private static final Comparator<Text> END_COMP = Comparator.nullsLast(Text::compareTo);

  /**
   * Find all the tablets within the provided bulk load mapping range.
   */
  private List<TabletMetadata> findOverlappingTablets(KeyExtent loadRange,
      Iterator<TabletMetadata> tabletIter) {

    TabletMetadata currTablet = null;

    try {

      List<TabletMetadata> tablets = new ArrayList<>();
      currTablet = tabletIter.next();

      int cmp;

      // skip tablets until we find the prevEndRow of loadRange
      while ((cmp = PREV_COMP.compare(currTablet.getPrevEndRow(), loadRange.prevEndRow())) < 0) {
        currTablet = tabletIter.next();
      }

      if (cmp != 0) {
        throw new IllegalStateException(
            "Unexpected prev end row " + currTablet.getExtent() + " " + loadRange);
      }

      // we have found the first tablet in the range, add it to the list
      tablets.add(currTablet);

      // find the remaining tablets within the loadRange by
      // adding tablets to the list until the endRow matches the loadRange
      while ((cmp = END_COMP.compare(currTablet.getEndRow(), loadRange.endRow())) < 0) {
        currTablet = tabletIter.next();
        tablets.add(currTablet);
      }

      if (cmp != 0) {
        throw new IllegalStateException("Unexpected end row " + currTablet + " " + loadRange);
      }

      return tablets;
    } catch (NoSuchElementException e) {
      NoSuchElementException ne2 = new NoSuchElementException(
          "Failed to find overlapping tablets " + currTablet + " " + loadRange);
      ne2.initCause(e);
      throw ne2;
    }
  }
}
