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

//TODO may need to make this an interface so it can have methods to get summaries and such
public class FileInfo {

  private final long estimatedSize;
  private final long estimatedEntries;

  public FileInfo(long estimatedSize, long estimatedEntries) {
    this.estimatedSize = estimatedSize;
    this.estimatedEntries = estimatedEntries;
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public long getEstimatedEntries() {
    return estimatedEntries;
  }
}
