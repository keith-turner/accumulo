package org.apache.accumulo.tserver.compactions;

import org.apache.accumulo.core.data.AbstractId;

public class CompactionExecutorId extends AbstractId {
  private static final long serialVersionUID = 1L;

  protected CompactionExecutorId(String canonical) {
    super(canonical);
  }
}
