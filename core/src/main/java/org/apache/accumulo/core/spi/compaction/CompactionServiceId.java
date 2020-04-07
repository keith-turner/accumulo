package org.apache.accumulo.core.spi.compaction;

import org.apache.accumulo.core.data.AbstractId;

public class CompactionServiceId extends AbstractId<CompactionServiceId> {
  private static final long serialVersionUID = 1L;

  private CompactionServiceId(String canonical) {
    super(canonical);
  }

  public static CompactionServiceId of(String canonical) {
    return new CompactionServiceId(canonical);
  }
}
