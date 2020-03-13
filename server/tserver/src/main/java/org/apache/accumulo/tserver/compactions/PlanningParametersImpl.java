package org.apache.accumulo.tserver.compactions;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner.PlanningParameters;
import org.apache.accumulo.core.spi.compaction.FileInfo;
import org.apache.accumulo.core.spi.compaction.SubmittedJob;

public class PlanningParametersImpl implements PlanningParameters {

  private Compactable compactable;
  private Collection<SubmittedJob> submittedJobs;

  public PlanningParametersImpl(Compactable compactable, Collection<SubmittedJob> submittedJobs) {
    this.compactable = compactable;
    this.submittedJobs = submittedJobs;
  }

  @Override
  public TableId getTableId() {
    // TODO Auto-generated method stub
    return compactable.getTableId();
  }

  @Override
  public Map<String,String> getHints() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<URI,FileInfo> getFiles() {
    return compactable.getFiles();
  }

  @Override
  public Collection<SubmittedJob> getSubmittedJobs() {
    return submittedJobs;
  }

}
