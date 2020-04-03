package org.apache.accumulo.tserver.compactions;

public interface ExecutorManager {
  public CompactionExecutorId createExecutor(String name, int threads);
}
