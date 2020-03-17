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
import java.util.Map;

public class CompactionJob {

  private final long priority;
  private final String executor;
  private final Map<URI,FileInfo> files;
  // TODO compaction config

  public CompactionJob(long priority, String executor, Map<URI,FileInfo> files) {
    this.priority = priority;
    this.executor = executor;
    this.files = files;
  }

  public long getPriority() {
    return priority;
  }

  public String getExecutor() {
    return executor;
  }

  public Map<URI,FileInfo> getFiles() {
    return files;
  }

  @Override
  public String toString() {
    return "CompactionJob [priority=" + priority + ", executor=" + executor + ", files=" + files
        + "]";
  }
}
