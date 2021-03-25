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
namespace java org.apache.accumulo.core.compaction.thrift
namespace cpp org.apache.accumulo.core.compaction.thrift

include "client.thrift"
include "data.thrift"
include "security.thrift"
include "tabletserver.thrift"
include "trace.thrift"

enum CompactionState {
  # Coordinator should set state to ASSIGNED when getCompactionJob is called by Compactor
  ASSIGNED
  # Compactor should set state to STARTED when compaction has successfully begun
  STARTED
  # Compactor can call repeatedly with an updated message to reflect percentage complete
  IN_PROGRESS
  # Compactor should set state to SUCCEEDED when compaction job has successfully finished
  SUCCEEDED
  # Compactor should set state to FAILED when compaction job fails, message should be mandatory
  FAILED
  # Compactor should set state to CANCELLED to acknowledge that it has stopped compacting 
  CANCELLED
}

struct Status {
  1:i64 timestamp
  2:string externalCompactionId
  3:string compactor
  4:CompactionState state
  5:string message
}

exception UnknownCompactionIdException {}

service CompactionCoordinator {

  /*
   * Called by TabletServer to cancel a compaction for a tablet
   */
  void cancelCompaction(
    1:string externalCompactionId
  ) throws (
    1:UnknownCompactionIdException e
  )

  /*
   * Called by Compactor on successful completion of compaction job
   */
  void compactionCompleted(
    1:string externalCompactionId
    2:data.TKeyExtent extent
    3:tabletserver.CompactionStats stats
  ) throws (
    1:UnknownCompactionIdException e
  )
  
  /*
   * Called by Compactor to get the next compaction job
   */
  tabletserver.TExternalCompactionJob getCompactionJob(
    1:string queueName
    2:string compactor
  )

  /*
   * Called by TabletServer (or CLI) to get current status of compaction for a tablet
   */
  list<Status> getCompactionStatus(
    1:string externalCompactionId
  ) throws (
    1:UnknownCompactionIdException e
  )
     
  /*
   * Called by Compactor to update the Coordinator with the state of the compaction
   */
  void updateCompactionStatus(
    1:string externalCompactionId
    2:CompactionState state
    3:string message
    4:i64 timestamp
  ) throws (
    1:UnknownCompactionIdException e
  )
  
  /*
   *  TODO need a function to report failed compactions
   */

}

struct TRunningCompaction {
  1:string externalCompactionId
  2:data.TKeyExtent extent
}

service Compactor {

  /*
   * Called by Coordinator to instruct the Compactor to stop working on the compaction.
   */
  void cancel(
    1:string externalCompactionId
  ) throws (
    1:UnknownCompactionIdException e
  )

  TRunningCompaction getRunningCompaction(
    1:trace.TInfo tinfo
    2:security.TCredentials credentials
  ) throws (
    1:client.ThriftSecurityException sec
  )
}
