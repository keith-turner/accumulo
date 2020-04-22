package org.apache.accumulo.core.spi.compaction;

import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;

/**
 * An immutable object that describes what files to compact and where to compact them.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public interface CompactionJob {

  long getPriority();

  /**
   * @return The executor to run the job.
   */
  CompactionExecutorId getExecutor();

  /**
   * @return The files to compact
   */
  Set<CompactableFile> getFiles();

  /**
   * @return The kind of compaction this is.
   */
  CompactionKind getKind();

}
