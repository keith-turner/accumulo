/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.client;

import org.apache.hadoop.net.DNSToSwitchMapping;

/**
 * 
 */
public class BatchWriterConfig {
  private long maxMemory = 10000000;
  private long maxLatency = 120000;
  private int maxWriteThreads = 3;
  private DNSToSwitchMapping mapping = null;
  
  public long getMaxMemory() {
    return maxMemory;
  }
  
  public void setMaxMemory(long maxMemory) {
    this.maxMemory = maxMemory;
  }
  
  public long getMaxLatency() {
    return maxLatency;
  }
  
  public void setMaxLatency(long maxLatency) {
    this.maxLatency = maxLatency;
  }
  
  public int getMaxWriteThreads() {
    return maxWriteThreads;
  }
  
  public void setMaxWriteThreads(int maxWriteThreads) {
    this.maxWriteThreads = maxWriteThreads;
  }
  
  public DNSToSwitchMapping getMapping() {
    return mapping;
  }
  
  // TODO a better name
  public void setMapping(DNSToSwitchMapping mapping) {
    this.mapping = mapping;
  }
  
}
