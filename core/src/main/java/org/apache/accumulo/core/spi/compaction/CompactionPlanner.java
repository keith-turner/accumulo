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

    Collection<CompactableFile> getAll();

    Collection<CompactableFile> getCandidates();

    /**
     * @return jobs that are currently running
     */
    Collection<CompactionJob> getRunningCompactions();

  }

  CompactionPlan makePlan(PlanningParameters params);
}
