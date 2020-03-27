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

import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compactions.Compactable;
import org.apache.accumulo.tserver.compactions.CompactionJob;
import org.apache.accumulo.tserver.compactions.CompactionService.Id;
import org.apache.accumulo.tserver.compactions.CompactionType;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.tablet.Compactor.CompactionEnv;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class CompactableImpl implements Compactable {

  private final Tablet tablet;

  private Set<StoredTabletFile> compactingFiles = new HashSet<>();

  // TODO would be better if this set were persistent
  private Set<StoredTabletFile> userFiles = new HashSet<>();

  private Set<StoredTabletFile> choppingFiles = new HashSet<>();

  // track files produced by compactions of this tablet, those are considered chopped
  private Set<StoredTabletFile> choppedFiles = new HashSet<>();

  // status of special compactions
  private enum SpecialStatus {
    NEW, SELECTING, SELECTED, NOT_ACTIVE
  }

  private SpecialStatus userStatus = SpecialStatus.NOT_ACTIVE;
  private SpecialStatus chopStatus = SpecialStatus.NOT_ACTIVE;

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

  void initiateChop() {
    Set<StoredTabletFile> chopCandidates = new HashSet<>();
    synchronized (this) {
      if (chopStatus == SpecialStatus.NOT_ACTIVE) {
        // TODO may want to do nothing instead of throw exception
        Preconditions.checkState(userStatus == SpecialStatus.NOT_ACTIVE);
        chopStatus = SpecialStatus.SELECTING;
        choppingFiles.clear();

        chopCandidates.addAll(tablet.getDatafiles().keySet());
        // any files currently compacting will be chopped
        chopCandidates.removeAll(compactingFiles);
        chopCandidates.removeAll(choppedFiles);

      } else {
        // TODO
        return;
      }
    }

    Set<StoredTabletFile> chopSelections = selectChopFiles(chopCandidates);
    if (chopSelections.isEmpty()) {
      markChopped();
    }

    synchronized (this) {
      Preconditions.checkState(chopStatus == SpecialStatus.SELECTING);
      if (chopSelections.isEmpty()) {
        chopStatus = SpecialStatus.NOT_ACTIVE;
      } else {
        chopStatus = SpecialStatus.SELECTED;
        choppingFiles.addAll(chopSelections);
      }

      // any candidates that were analyzed and found not needing a chop can be considered chopped
      choppedFiles.addAll(Sets.difference(chopCandidates, chopSelections));
    }

  }

  private void markChopped() {
    // TODO work into compaction mutation
    MetadataTableUtil.chopped(tablet.getTabletServer().getContext(), getExtent(),
        tablet.getTabletServer().getLock());
    tablet.getTabletServer()
        .enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.CHOPPED, getExtent()));
  }

  private Set<StoredTabletFile> selectChopFiles(Set<StoredTabletFile> chopCandidates) {
    try {
      var firstAndLastKeys = getFirstAndLastKeys(chopCandidates);
      return findChopFiles(getExtent(), firstAndLastKeys, chopCandidates);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void chopCompactionCompleted(CompactionJob job) {
    boolean markChopped = false;
    synchronized (this) {
      Preconditions.checkState(chopStatus == SpecialStatus.SELECTED);
      Preconditions.checkState(choppingFiles.containsAll(job.getFiles()));

      choppingFiles.removeAll(job.getFiles());

      if (choppingFiles.isEmpty()) {
        chopStatus = SpecialStatus.NOT_ACTIVE;
        markChopped = true;
      }
    }

    if (markChopped)
      markChopped();
  }

  private Map<StoredTabletFile,Pair<Key,Key>> getFirstAndLastKeys(Set<StoredTabletFile> allFiles)
      throws IOException {
    final Map<StoredTabletFile,Pair<Key,Key>> result = new HashMap<>();
    final FileOperations fileFactory = FileOperations.getInstance();
    final VolumeManager fs = tablet.getTabletServer().getFileSystem();
    for (StoredTabletFile file : allFiles) {
      FileSystem ns = fs.getFileSystemByPath(file.getPath());
      try (FileSKVIterator openReader = fileFactory.newReaderBuilder()
          .forFile(file.getPathStr(), ns, ns.getConf(), tablet.getContext().getCryptoService())
          .withTableConfiguration(tablet.getTableConfiguration()).seekToBeginning().build()) {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<>(first, last));
      }
    }
    return result;
  }

  Set<StoredTabletFile> findChopFiles(KeyExtent extent,
      Map<StoredTabletFile,Pair<Key,Key>> firstAndLastKeys, Collection<StoredTabletFile> allFiles) {
    Set<StoredTabletFile> result = new HashSet<>();

    for (StoredTabletFile file : allFiles) {
      Pair<Key,Key> pair = firstAndLastKeys.get(file);
      Key first = pair.getFirst();
      Key last = pair.getSecond();
      // If first and last are null, it's an empty file. Add it to the compact set so it goes
      // away.
      if ((first == null && last == null) || (first != null && !extent.contains(first.getRow()))
          || (last != null && !extent.contains(last.getRow()))) {
        result.add(file);
      }

    }
    return result;
  }

  /**
   * Tablet calls this signal a user compaction should run
   */
  void initiateUserCompaction(long compactionId, Selector selectionFunction) {
    synchronized (this) {

      if (userStatus == SpecialStatus.NOT_ACTIVE) {
        // chop and user compactions should be mutually exclusive... except for canceled compactions
        // and delayed threads/rpcs...
        // TODO may want to do nothing instead of throw exception
        Preconditions.checkState(chopStatus == SpecialStatus.NOT_ACTIVE);
        userStatus = SpecialStatus.NEW;
        userFiles.clear();
        this.selectionFunction = selectionFunction;
        this.compactionId = compactionId;
      } else {
        // TODO
        return;
      }
    }

    selectUserFiles();

  }

  private void selectUserFiles() {
    synchronized (this) {
      if (userStatus == SpecialStatus.NEW && compactingFiles.isEmpty()) {
        userFiles.clear();
        userFiles.addAll(tablet.getDatafiles().keySet());
        userStatus = SpecialStatus.SELECTING;
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
          userStatus = SpecialStatus.NOT_ACTIVE;
        }
      } else {
        synchronized (this) {
          userStatus = SpecialStatus.SELECTED;
          userFiles.addAll(selectedFiles);
        }
      }

    } catch (Exception e) {
      synchronized (this) {
        userStatus = SpecialStatus.NEW;
        userFiles.clear();
      }

      // TODO
      e.printStackTrace();
    }

  }

  private synchronized void userCompactionCompleted(CompactionJob job, StoredTabletFile newFile) {
    Preconditions.checkArgument(job.getType() == CompactionType.USER);
    Preconditions.checkState(userFiles.containsAll(job.getFiles()));
    Preconditions.checkState(userStatus == SpecialStatus.SELECTED);

    userFiles.removeAll(job.getFiles());

    if (userFiles.isEmpty()) {
      userStatus = SpecialStatus.NOT_ACTIVE;
    } else {
      userFiles.add(newFile);
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
        switch (userStatus) {
          case NOT_ACTIVE:
            return Optional.of(new Compactable.Files(files, type,
                Sets.difference(files.keySet(), compactingCopy), compactingCopy));
          case NEW:
          case SELECTING:
            return Optional.of(new Compactable.Files(files, type, Set.of(), compactingCopy));
          case SELECTED: {
            Set<StoredTabletFile> candidates = new HashSet<>(files.keySet());
            candidates.removeAll(compactingCopy);
            candidates.removeAll(userFiles);
            candidates = Collections.unmodifiableSet(candidates);
            return Optional.of(new Compactable.Files(files, type, candidates, compactingCopy));
          }
          default:
            throw new AssertionError();
        }
      case USER:
        switch (userStatus) {
          case NOT_ACTIVE:
          case NEW:
          case SELECTING:
            return Optional.of(new Compactable.Files(files, type, Set.of(), compactingCopy));
          case SELECTED:
            return Optional
                .of(new Compactable.Files(files, type, Set.copyOf(userFiles), compactingCopy));
          default:
            throw new AssertionError();
        }
      case CHOP:
        switch (chopStatus) {
          case NOT_ACTIVE:
          case NEW:
          case SELECTING:
            return Optional.of(new Compactable.Files(files, type, Set.of(), compactingCopy));
          case SELECTED:
            return Optional
                .of(new Compactable.Files(files, type, Set.copyOf(choppingFiles), compactingCopy));
        }
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void compact(Id service, CompactionJob job) {

    synchronized (this) {
      if (!service.equals(myService))
        return;

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
          // TODO check for service change????
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
        // TODO move metadata update to own method which can rollback changes after compaction
        if (job.getType() == CompactionType.USER && userFiles.equals(job.getFiles())) {
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

        // TODO this tracking feels a bit iffy
        if (metaFile != null) {
          choppedFiles.add(metaFile);
          choppedFiles.removeAll(job.getFiles());
        }
      }

      if (job.getType() == CompactionType.USER && metaFile != null)
        userCompactionCompleted(job, metaFile);// TODO what if it failed?
      else if (job.getType() == CompactionType.CHOP)
        chopCompactionCompleted(job);
      else
        selectUserFiles();
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
