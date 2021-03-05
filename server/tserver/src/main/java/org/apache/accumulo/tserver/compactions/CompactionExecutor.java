package org.apache.accumulo.tserver.compactions;

import java.util.function.Consumer;

import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;

public interface CompactionExecutor {

  SubmittedJob submit(CompactionServiceId csid, CompactionJob job, Compactable compactable,
      Consumer<Compactable> completionCallback);

  int getCompactionsRunning();

  int getCompactionsQueued();

  void stop();

}
