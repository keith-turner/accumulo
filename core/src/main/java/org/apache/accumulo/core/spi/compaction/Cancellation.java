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

import java.util.EnumSet;

import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;

public class Cancellation {
  private final CompactionId compactionId;
  private final EnumSet<Status> statusesToCancel;

  private static final EnumSet<Status> QUEUED = EnumSet.of(Status.QUEUED);
  private static final EnumSet<Status> ALL = EnumSet.allOf(Status.class);

  private Cancellation(CompactionId compactionId, EnumSet<Status> statusesToCancel) {
    this.compactionId = compactionId;
    this.statusesToCancel = statusesToCancel;
  }

  public CompactionId getCompactionId() {
    return compactionId;
  }

  public EnumSet<Status> getStatusesToCancel() {
    return statusesToCancel;
  }

  public static Cancellation cancelQueued(CompactionId cid) {
    return new Cancellation(cid, QUEUED);
  }

  public static Cancellation cancelAny(CompactionId cid) {
    return new Cancellation(cid, ALL);
  }

  @Override
  public String toString() {
    return "Cancellation [compactionId=" + compactionId + ", statusesToCancel=" + statusesToCancel
        + "]";
  }

}
