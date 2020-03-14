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
import java.util.List;

public class CompactionPlan {

  private final Collection<Cancellation> cancellations;
  private final Collection<CompactionJob> jobs;

  public CompactionPlan() {
    this.cancellations = List.of();
    this.jobs = List.of();
  }

  public CompactionPlan(Collection<CompactionJob> jobs, Collection<Cancellation> cancellations) {
    this.jobs = List.copyOf(jobs);
    this.cancellations = List.copyOf(cancellations);
  }

  public Collection<CompactionJob> getJobs() {
    return jobs;
  }

  public Collection<Cancellation> getCancellations() {
    return cancellations;
  }

  @Override
  public String toString() {
    return "jobs: " + jobs + " cancellations: " + cancellations;
  }
}
