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

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

public interface CompactionManager {
  public interface InitParameters {
    Map<String,String> getOptions();

    ServiceEnvironment getServiceEnv();

    Map<String,CompactionExecutor> getCompactionExecutors();
  }

  void init(InitParameters params);

  public interface PlanningParameters {
    TableId getTableId();

    Map<String,String> getHints();

    Map<URI,FileInfo> getFiles();

    Collection<SubmittedJob> getSubmittedJobs();
  }

  CompactionPlan makePlan(PlanningParameters params);

  public interface UserPlanningParameters extends PlanningParameters {

    Map<String,String> getExecutionHints();

    /**
     * These are the files a user requires compaction for. These may be a subset of
     * {@link #getFiles()}. If multiple passes are required to compact these files, its ok to return
     * a subset of the files. Later a plan will be requested for the remaining files. However, all
     * files must eventually be compacted.
     */
    Map<URI,FileInfo> getUserFiles();
  }

  CompactionPlan makePlanForUser(UserPlanningParameters params);

}
