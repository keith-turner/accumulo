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

import org.apache.accumulo.core.data.AbstractId;

import com.google.common.base.Preconditions;

/**
 * A unique identifier for a a compaction executor that a {@link CompactionPlanner} can schedule
 * compactions on using a {@link CompactionJob}.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */
public class CompactionExecutorId extends AbstractId<CompactionExecutorId> {
  private static final long serialVersionUID = 1L;

  private CompactionExecutorId(String canonical) {
    super(canonical);
  }

  // CBUG maybe all of the following methods should not be in SPI
  public boolean isExernalId() {
    return canonical().startsWith("e.");
  }

  public String getExernalName() {
    Preconditions.checkState(isExernalId());
    return canonical().substring("e.".length());
  }

  public static CompactionExecutorId internalId(CompactionServiceId csid, String executorName) {
    return new CompactionExecutorId("i." + csid + "." + executorName);
  }

  public static CompactionExecutorId externalId(String executorName) {
    return new CompactionExecutorId("e." + executorName);
  }
}
