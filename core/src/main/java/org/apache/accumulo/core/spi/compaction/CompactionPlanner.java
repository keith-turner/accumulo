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
package org.apache.accumulo.core.spi.compaction;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

/**
 * Plans compaction work for a compaction service.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public interface CompactionPlanner {

  public interface InitParameters {
    ServiceEnvironment getServiceEnvironment();

    Map<String,String> getOptions();

    String getFullyQualifiedOption(String key);

    ExecutorManager getExecutorManager();
  }

  public void init(InitParameters params);

  public interface PlanningParameters {

    TableId getTableId();

    ServiceEnvironment getServiceEnvironment();

    CompactionKind getKind();

    double getRatio();

    /**
     * @return the set of all files a tablet has.
     */
    Collection<CompactableFile> getAll();

    /**
     * @return the set of files that could be compacted depending on what {@link #getKind()}
     *         returns.
     */
    Collection<CompactableFile> getCandidates();

    /**
     * @return jobs that are currently running
     */
    Collection<CompactionJob> getRunningCompactions();
  }

  /**
   * <p>
   * Plan what work a compaction service should do. The kind of compaction returned by
   * {@link PlanningParameters#getKind()} determines what must be done with the files returned by
   * {@link PlanningParameters#getCandidates()}. The following are the expectations for the
   * candidates for each kind.
   *
   * <ul>
   * <li>{@link CompactionKind#SYSTEM} The planner is not required to do anything with the
   * candidates and can choose to compact zero or more of them. The candidates may represent a
   * subset of all the files in the case where a user compaction is in progress or other compactions
   * are running.
   * <li>{@link CompactionKind#USER} and {@link CompactionKind#SELECTED} The planner is required to
   * eventually compact all candidates. Its ok to return a compaction plan that compacts a subset.
   * When the planner compacts a subset, it will eventually be called again later. When it is called
   * later the candidates will contain the files it did not compact and the results of any previous
   * compactions it scheduled. The planner must eventually compact all of the files in the candidate
   * set down to a single file. The compaction service will keep calling the planner until it does.
   * <li>{@link CompactionKind#CHOP} The planner is required to eventually compact all candidates.
   * One major difference with USER compactions is this kind is not required to compact all files to
   * a single file. It is ok to return a compaction plan that compacts a subset of the candidates.
   * When the planner compacts a subset, it will eventually be called later. When it is called later
   * the candidates will contain the files it did not compact.
   * </ul>
   *
   * <p>
   * When a planner returns a compactions plan, task will be queued on executors. Previously queued
   * task that do not match the latest plan are removed. The planner is called periodically,
   * whenever a new file is added, and whenever a compaction finishes.
   */
  CompactionPlan makePlan(PlanningParameters params);
}
