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
