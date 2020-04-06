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
package org.apache.accumulo.tserver.compactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

/**
 * Interface between compaction service and tablet.
 */
public interface Compactable {

  public static class Files {

    public final Collection<CompactableFile> allFiles;
    public final CompactionKind kind;
    public final Collection<CompactableFile> candidates;
    public final Collection<Collection<CompactableFile>> compacting;

    public Files(SortedMap<StoredTabletFile,DataFileValue> allFiles, CompactionKind kind,
        Set<StoredTabletFile> candidates, Collection<Set<StoredTabletFile>> compacting) {

      // TODO can the copies be avoided.?

      this.allFiles = Collections.unmodifiableSet(allFiles.entrySet().stream()
          .map(entry -> new CompactableFileImpl(entry.getKey(), entry.getValue()))
          .collect(Collectors.toSet()));
      this.kind = kind;
      this.candidates = Collections.unmodifiableSet(candidates.stream()
          .map(stf -> new CompactableFileImpl(stf, allFiles.get(stf))).collect(Collectors.toSet()));
      Collection<Collection<CompactableFile>> compactingTmp = new ArrayList<>();

      compacting.forEach(cf -> {
        compactingTmp.add(Collections
            .unmodifiableSet(cf.stream().map(stf -> new CompactableFileImpl(stf, allFiles.get(stf)))
                .collect(Collectors.toSet())));
      });

      this.compacting = Collections.unmodifiableCollection(compactingTmp);
    }

    @Override
    public String toString() {
      return "Files [allFiles=" + allFiles + ", type=" + kind + ", candidates=" + candidates
          + ", compacting=" + compacting + "]";
    }

  }

  TableId getTableId();

  KeyExtent getExtent();

  Optional<Files> getFiles(CompactionService.Id service, CompactionKind kind);

  // void compact(CompactionJob compactionJob);

  void compact(CompactionService.Id service, CompactionJob job);

  CompactionService.Id getConfiguredService(CompactionKind kind);

  double getCompactionRatio();

  void registerNewFilesCallback(Consumer<Compactable> callback);
}
