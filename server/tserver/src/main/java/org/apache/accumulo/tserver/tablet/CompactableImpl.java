/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionJob;
import org.apache.accumulo.tserver.compactions.CompactionService.Id;
import org.apache.accumulo.tserver.compactions.CompactionType;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class CompactableImpl implements Compactable {

  private final Tablet tablet;

  private Set<StoredTabletFile> compactingFiles = new HashSet<>();

  // TODO would be better if this set were persistent
  private Set<StoredTabletFile> userCompactionFiles = new HashSet<>();

  private enum UserCompactionStatus {
    NEW, SELECTING, SELECTED, NOT_ACTIVE
  }

  private UserCompactionStatus userCompactionStatus = UserCompactionStatus.NOT_ACTIVE;

  private Selector selectionFunction;
  private long compactionId;

  public CompactableImpl(Tablet tablet) {
    this.tablet = tablet;
  }

  // TODO this is a temporary hack and should be removed

  private final Id myService = Id.of("default");

  public static interface Selector {
    Collection<StoredTabletFile> select(Map<StoredTabletFile,DataFileValue> files);
  }

  /**
   * Tablet calls this signal a user compaction should run
   */
  synchronized void initiateUserCompaction(long compactionId, Selector selectionFunction) {
    synchronized (this) {
      if (userCompactionStatus == UserCompactionStatus.NOT_ACTIVE) {
        userCompactionStatus = UserCompactionStatus.NEW;
        userCompactionFiles.clear();
        this.selectionFunction = selectionFunction;
        this.compactionId = compactionId;
      } else {
        // TODO
      }
    }

    selectFiles();

  }

  private synchronized void selectFiles() {
    synchronized (this) {
      if (userCompactionStatus == UserCompactionStatus.NEW && compactingFiles.isEmpty()) {
        userCompactionFiles.clear();
        userCompactionFiles.addAll(tablet.getDatafiles().keySet());
        userCompactionStatus = UserCompactionStatus.SELECTING;
      } else {
        return;
      }
    }

    try {
      // run selection outside of sync
      var selectedFiles = selectionFunction.select(tablet.getDatafiles());

      if (selectedFiles.isEmpty()) {

        // TODO seems like this should be set after the metadata update.. was before in the exisitng
        // code
        tablet.setLastCompactionID(compactionId);

        MetadataTableUtil.updateTabletCompactID(tablet.getExtent(), compactionId,
            tablet.getTabletServer().getContext(), tablet.getTabletServer().getLock());

        synchronized (this) {
          userCompactionStatus = UserCompactionStatus.NOT_ACTIVE;
        }
      } else {
        synchronized (this) {
          userCompactionStatus = UserCompactionStatus.SELECTED;
          userCompactionFiles.addAll(selectedFiles);
        }
      }

    } catch (Exception e) {
      synchronized (this) {
        userCompactionStatus = UserCompactionStatus.NEW;
        userCompactionFiles.clear();
      }

      // TODO
      e.printStackTrace();
    }

  }

  private synchronized void userCompactionCompleted(CompactionJob job, StoredTabletFile newFile) {
    Preconditions.checkArgument(job.getType() == CompactionType.USER);
    Preconditions.checkState(userCompactionFiles.containsAll(job.getFiles()));
    Preconditions.checkState(userCompactionStatus == UserCompactionStatus.SELECTED);

    userCompactionFiles.removeAll(job.getFiles());

    if (userCompactionFiles.isEmpty()) {
      userCompactionStatus = UserCompactionStatus.NOT_ACTIVE;
    } else {
      userCompactionFiles.add(newFile);
    }
  }

  @Override
  public TableId getTableId() {
    return getExtent().getTableId();
  }

  @Override
  public KeyExtent getExtent() {
    return tablet.getExtent();
  }

  @Override
  public synchronized Optional<Files> getFiles(Id service, CompactionType type) {
    // TODO not consistently obtaing tablet state
    if (tablet.isClosing() || tablet.isClosed() || !service.equals(myService))
      return Optional.empty();

    // TODO get consistent snapshot of compacting files and existing files... race condition with
    // current hack
    // TODO check service
    // intentionally copy before getting snapshot of tablet files.
    var compactingCopy = Set.copyOf(compactingFiles);
    var files = tablet.getDatafiles();

    switch (type) {
      case MAINTENANCE:
        switch (userCompactionStatus) {
          case NOT_ACTIVE:
            return Optional.of(new Compactable.Files(files, type,
                Sets.difference(files.keySet(), compactingCopy), compactingCopy));
          case NEW:
          case SELECTING:
            return Optional.of(new Compactable.Files(files, type, Set.of(), compactingCopy));
          case SELECTED: {
            Set<StoredTabletFile> candidates = new HashSet<>(files.keySet());
            candidates.removeAll(compactingCopy);
            candidates.removeAll(userCompactionFiles);
            candidates = Collections.unmodifiableSet(candidates);
            return Optional.of(new Compactable.Files(files, type, candidates, compactingCopy));
          }
          default:
            throw new AssertionError();
        }
      case USER:
        switch (userCompactionStatus) {
          case NOT_ACTIVE:
          case NEW:
          case SELECTING:
            return Optional.of(new Compactable.Files(files, type, Set.of(), compactingCopy));
          case SELECTED:
            return Optional.of(new Compactable.Files(files, type, Set.copyOf(userCompactionFiles),
                compactingCopy));
          default:
            throw new AssertionError();
        }
      case CHOP:
        // TODO
        throw new UnsupportedOperationException();
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void compact(Id service, CompactionJob job) {
    // TODO could be closed... maybe register and deregister compaction

    if (!service.equals(myService)) {
      return;
    }

    synchronized (this) {

      if (Collections.disjoint(compactingFiles, job.getFiles()))
        compactingFiles.addAll(job.getFiles());
      else
        return; // TODO log an error?
    }
    // TODO only add if not in set!
    StoredTabletFile metaFile = null;
    try {
      CompactionEnv cenv = new CompactionEnv() {
        @Override
        public boolean isCompactionEnabled() {
          return !tablet.isClosing();
        }

        @Override
        public IteratorScope getIteratorScope() {
          return IteratorScope.majc;
        }

        @Override
        public RateLimiter getReadLimiter() {
          return tablet.getTabletServer().getMajorCompactionReadLimiter();
        }

        @Override
        public RateLimiter getWriteLimiter() {
          return tablet.getTabletServer().getMajorCompactionWriteLimiter();
        }
      };

      // TODO
      int reason = MajorCompactionReason.NORMAL.ordinal();

      AccumuloConfiguration tableConfig = tablet.getTableConfiguration();

      SortedMap<StoredTabletFile,DataFileValue> allFiles = tablet.getDatafiles();
      HashMap<StoredTabletFile,DataFileValue> compactFiles = new HashMap<>();
      job.getFiles().forEach(file -> compactFiles.put((StoredTabletFile) file, allFiles.get(file)));

      // TODO this is done outside of sync block
      boolean propogateDeletes = !allFiles.keySet().equals(compactFiles.keySet());

      TabletFile newFile = tablet.getNextMapFilename(!propogateDeletes ? "A" : "C");
      TabletFile compactTmpName = new TabletFile(new Path(newFile.getMetaInsert() + "_tmp"));

      // TODO user iters
      List<IteratorSetting> iters = List.of();

      // check as late as possible
      if (tablet.isClosing() || tablet.isClosed())
        return;

      Compactor compactor = new Compactor(tablet.getContext(), tablet, compactFiles, null,
          compactTmpName, propogateDeletes, cenv, iters, reason, tableConfig);

      var mcs = compactor.call();

      Long compactionId = null;
      synchronized (this) {
        // TODO this is really iffy in the face of failures!
        if (job.getType() == CompactionType.USER && userCompactionFiles.equals(job.getFiles())) {
          compactionId = this.compactionId;
        }

      }

      metaFile = tablet.getDatafileManager().bringMajorCompactionOnline(compactFiles.keySet(),
          compactTmpName, newFile, compactionId,
          new DataFileValue(mcs.getFileSize(), mcs.getEntriesWritten()));

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      synchronized (this) {
        compactingFiles.removeAll(job.getFiles());
      }
      if (job.getType() == CompactionType.USER && metaFile != null)
        userCompactionCompleted(job, metaFile);// TODO what if it failed?
      else
        selectFiles();
    }
  }

  @Override
  public Id getConfiguredService(CompactionType type) {
    // TODO
    return myService;
  }

  @Override
  public double getCompactionRatio() {
    // TODO
    return 2;
  }

}
