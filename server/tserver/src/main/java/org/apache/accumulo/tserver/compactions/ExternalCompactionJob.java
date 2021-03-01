package org.apache.accumulo.tserver.compactions;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.compaction.thrift.CompactionJob;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.CompactionReason;
import org.apache.accumulo.core.tabletserver.thrift.CompactionType;
import org.apache.accumulo.core.tabletserver.thrift.IteratorConfig;

public class ExternalCompactionJob {

  private Set<StoredTabletFile> jobFiles;
  private boolean propogateDeletes;
  private TabletFile compactTmpName;
  private KeyExtent extent;
  private Long compactionId;
  private long priority;
  private CompactionKind kind;
  private List<IteratorSetting> iters;

  public ExternalCompactionJob(Set<StoredTabletFile> jobFiles, boolean propogateDeletes,
      TabletFile compactTmpName, KeyExtent extent, Long compactionId, long priority,
      CompactionKind kind, List<IteratorSetting> iters) {
    this.jobFiles = jobFiles;
    this.propogateDeletes = propogateDeletes;
    this.compactTmpName = compactTmpName;
    this.extent = extent;
    this.compactionId = compactionId;
    this.priority = priority;
    this.kind = kind;
    this.iters = iters;
  }

  CompactionJob toThrift() {

    // TODO read and write rate
    int readRate = 0;
    int writeRate = 0;

    // TODO how are these two used?
    CompactionType type = propogateDeletes ? CompactionType.MAJOR : CompactionType.FULL;
    CompactionReason reason;
    switch (kind) {
      case USER:
        reason = CompactionReason.USER;
        break;
      case CHOP:
        reason = CompactionReason.CHOP;
        break;
      case SYSTEM:
      case SELECTOR:
        reason = CompactionReason.SYSTEM;
        break;
      default:
        throw new IllegalStateException();
    }

    IteratorConfig iteratorSettings = SystemIteratorUtil.toIteratorConfig(iters);

    List<String> files =
        jobFiles.stream().map(StoredTabletFile::getPathStr).collect(Collectors.toList());

    // TODO priority cast
    return new CompactionJob(null, null, compactionId, extent.toThrift(), files, (int) priority,
        readRate, writeRate, iteratorSettings, type, reason, compactTmpName.getPathStr());
  }
}
