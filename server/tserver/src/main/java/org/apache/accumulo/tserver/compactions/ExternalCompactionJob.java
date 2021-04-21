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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.InputFile;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionType;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;

public class ExternalCompactionJob {

  private Set<StoredTabletFile> jobFiles;
  private boolean propogateDeletes;
  private TabletFile compactTmpName;
  private KeyExtent extent;
  private ExternalCompactionId externalCompactionId;
  private long priority;
  private CompactionKind kind;
  private List<IteratorSetting> iters;

  public ExternalCompactionJob() {}

  public ExternalCompactionJob(Set<StoredTabletFile> jobFiles, boolean propogateDeletes,
      TabletFile compactTmpName, KeyExtent extent, ExternalCompactionId externalCompactionId,
      long priority, CompactionKind kind, List<IteratorSetting> iters) {
    this.jobFiles = Objects.requireNonNull(jobFiles);
    this.propogateDeletes = propogateDeletes;
    this.compactTmpName = Objects.requireNonNull(compactTmpName);
    this.extent = Objects.requireNonNull(extent);
    this.externalCompactionId = Objects.requireNonNull(externalCompactionId);
    this.priority = priority;
    this.kind = Objects.requireNonNull(kind);
    this.iters = Objects.requireNonNull(iters);
  }

  public TExternalCompactionJob toThrift() {

    // TODO read and write rate
    int readRate = 0;
    int writeRate = 0;

    // TODO how are these two used?
    TCompactionType type = propogateDeletes ? TCompactionType.MAJOR : TCompactionType.FULL;
    TCompactionReason reason;
    switch (kind) {
      case USER:
        reason = TCompactionReason.USER;
        break;
      case CHOP:
        reason = TCompactionReason.CHOP;
        break;
      case SYSTEM:
      case SELECTOR:
        reason = TCompactionReason.SYSTEM;
        break;
      default:
        throw new IllegalStateException();
    }

    IteratorConfig iteratorSettings = SystemIteratorUtil.toIteratorConfig(iters);

    // TODO what are things that are zeros below needed for
    List<InputFile> files = jobFiles.stream().map(stf -> new InputFile(stf.getPathStr(), 0, 0, 0))
        .collect(Collectors.toList());

    // CBUG there seem to be two CompactionKind thrift types
    // CBUG rename CompactionKind thrift type to TCompactionKind
    // TODO priority cast and compactionId cast... compactionId could be null I think
    return new TExternalCompactionJob(externalCompactionId.toString(), extent.toThrift(), files,
        (int) priority, readRate, writeRate, iteratorSettings, type, reason,
        compactTmpName.getPathStr(), propogateDeletes,
        org.apache.accumulo.core.tabletserver.thrift.TCompactionKind.valueOf(kind.name()));
  }

  public ExternalCompactionId getExternalCompactionId() {
    return externalCompactionId;
  }

  public KeyExtent getExtent() {
    return extent;
  }
}
