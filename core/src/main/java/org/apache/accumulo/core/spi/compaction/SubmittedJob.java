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

public class SubmittedJob {
  private final Status status;
  private final CompactionId id;
  private final CompactionJob job;
  private Type type;

  public enum Status {
    RUNNING, QUEUED
  }

  public enum Type {
    USER, SYSTEM
  }

  public SubmittedJob(CompactionJob job, CompactionId id, Status status, Type type) {
    this.job = job;
    this.id = id;
    this.status = status;
    this.type = type;
  }

  public CompactionJob getJob() {
    return job;
  }

  public CompactionId getId() {
    return id;
  }

  public Status getStatus() {
    return status;
  }

  public Type getRequester() {
    return type;
  }
}
