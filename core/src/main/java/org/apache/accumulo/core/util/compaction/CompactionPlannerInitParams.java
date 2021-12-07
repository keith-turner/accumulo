package org.apache.accumulo.core.util.compaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.ExecutorManager;

import com.google.common.base.Preconditions;

public class CompactionPlannerInitParams implements CompactionPlanner.InitParameters {

  private final Map<String,String> plannerOpts;
  private final Map<CompactionExecutorId,Integer> requestedExecutors;
  private final Set<CompactionExecutorId> requestedExternalExecutors;
  private final ServiceEnvironment senv;
  private final CompactionServiceId serviceId;

  public CompactionPlannerInitParams(CompactionServiceId serviceId, Map<String,String> plannerOpts,
      ServiceEnvironment senv) {
    this.serviceId = serviceId;
    this.plannerOpts = plannerOpts;
    this.requestedExecutors = new HashMap<>();
    this.requestedExternalExecutors = new HashSet<>();
    this.senv = senv;
  }

  @Override
  public ServiceEnvironment getServiceEnvironment() {
    return senv;
  }

  @Override
  public Map<String,String> getOptions() {
    return plannerOpts;
  }

  @Override
  public String getFullyQualifiedOption(String key) {
    return Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey() + serviceId + ".opts." + key;
  }

  @Override
  public ExecutorManager getExecutorManager() {
    return new ExecutorManager() {
      @Override
      public CompactionExecutorId createExecutor(String executorName, int threads) {
        Preconditions.checkArgument(threads > 0, "Positive number of threads required : %s",
            threads);
        var ceid = CompactionExecutorIdImpl.internalId(serviceId, executorName);
        Preconditions.checkState(!getRequestedExecutors().containsKey(ceid));
        getRequestedExecutors().put(ceid, threads);
        return ceid;
      }

      @Override
      public CompactionExecutorId getExternalExecutor(String name) {
        var ceid = CompactionExecutorIdImpl.externalId(name);
        Preconditions.checkArgument(!getRequestedExternalExecutors().contains(ceid),
            "Duplicate external executor for queue " + name);
        getRequestedExternalExecutors().add(ceid);
        return ceid;
      }
    };
  }

  public Map<CompactionExecutorId,Integer> getRequestedExecutors() {
    return requestedExecutors;
  }

  public Set<CompactionExecutorId> getRequestedExternalExecutors() {
    return requestedExternalExecutors;
  }
}
