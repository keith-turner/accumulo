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
package org.apache.accumulo.tserver.compactions;

import static java.util.stream.Collectors.toSet;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.spi.compaction.Cancellation;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionPlan;
import org.apache.accumulo.core.spi.compaction.CompactionPlanner;
import org.apache.accumulo.core.spi.compaction.FileInfo;
import org.apache.accumulo.core.spi.compaction.SubmittedJob.Status;

public class TieredCompactionManager implements CompactionPlanner {

  private double cRatio;

  public TieredCompactionManager(double compactionRatio) {
    this.cRatio = compactionRatio;
  }

  @Override
  public void init(InitParameters params) {
    // TODO Auto-generated method stub

  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    // TODO only create if needed in an elegant way.

    Map<URI,FileInfo> filesCopy = new HashMap<>(params.getFiles());

    // find minimum file size for running compactions
    OptionalLong minCompacting =
        params.getSubmittedJobs().stream().filter(sj -> sj.getStatus() == Status.RUNNING)
            .flatMap(sj -> sj.getJob().getFiles().values().stream())
            .mapToLong(FileInfo::getEstimatedSize).min();

    if (minCompacting.isPresent()) {
      // This is done to ensure that compactions over time result in the mimimum number of files.
      // See the gist for more info.
      filesCopy = getSmallestFilesWithSumLessThan(filesCopy, minCompacting.getAsLong());
    }

    // remove any files from consideration that are in use by a running compaction
    params.getSubmittedJobs().stream().filter(sj -> sj.getStatus() == Status.RUNNING)
        .flatMap(sj -> sj.getJob().getFiles().keySet().stream()).forEach(filesCopy::remove);

    Set<URI> group = findMapFilesToCompact(filesCopy, cRatio);

    if (group.size() > 0) {
      List<Cancellation> cancellations = new ArrayList<>();

      // find all files related to queued jobs
      Set<URI> queued =
          params.getSubmittedJobs().stream().filter(sj -> sj.getStatus() == Status.QUEUED)
              .flatMap(sj -> sj.getJob().getFiles().keySet().stream()).collect(toSet());

      if (!queued.isEmpty()) {
        if (queued.equals(group)) {
          // currently queued jobs is the same set of files we want to compact, so just use that
          return new CompactionPlan();
        } else {
          // currently queued jobs are different than what we want to compact, so cancel them
          params.getSubmittedJobs().stream().filter(sj -> sj.getStatus() == Status.QUEUED)
              .map(sj -> Cancellation.cancelQueued(sj.getId())).forEach(cancellations::add);
        }
      }

      // TODO do we want to queue a job to an executor if we already have something running there??
      // determine which executor to use based on the size of the files
      String executor = getExecutor(
          group.stream().mapToLong(uri -> params.getFiles().get(uri).getEstimatedSize()).sum());

      Map<URI,FileInfo> filesToCompact = new HashMap<>();
      group.forEach(uri -> filesToCompact.put(uri, params.getFiles().get(uri)));
      CompactionJob job = new CompactionJob(params.getFiles().size(), executor, filesToCompact);

      return new CompactionPlan(List.of(job), cancellations);

    }

    return new CompactionPlan();
  }

  @Override
  public CompactionPlan makePlanForUser(UserPlanningParameters params) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Find the largest set of small files to compact.
   *
   * <p>
   * See https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
   */
  public static Set<URI> findMapFilesToCompact(Map<URI,FileInfo> files, double ratio) {
    if (files.size() <= 1)
      return Collections.emptySet();

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<Entry<URI,FileInfo>> sortedFiles = sortByFileSize(files);

    // index into sortedFiles, everything at and below this index is a good set of files to compact
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getValue().getEstimatedSize();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getValue().getEstimatedSize();
      sum += currSize;

      if (currSize * ratio < sum) {
        goodIndex = c;

        // look ahead to the next file to see if this a good stopping point
        if (c + 1 < sortedFiles.size()) {
          long nextSize = sortedFiles.get(c + 1).getValue().getEstimatedSize();
          boolean nextMeetsCR = nextSize * ratio < nextSize + sum;

          if (!nextMeetsCR && sum < nextSize) {
            // These two conditions indicate the largest set of small files to compact was found, so
            // stop looking.
            break;
          }
        }
      }
    }

    if (goodIndex == -1)
      return Collections.emptySet();

    return sortedFiles.subList(0, goodIndex + 1).stream().map(Entry<URI,FileInfo>::getKey)
        .collect(toSet());
  }

  public static Map<URI,FileInfo> getSmallestFilesWithSumLessThan(Map<URI,FileInfo> files,
      long cutoff) {
    List<Entry<URI,FileInfo>> sortedFiles = sortByFileSize(files);

    HashMap<URI,FileInfo> ret = new HashMap<>();

    long sum = 0;

    for (int index = 0; index < sortedFiles.size(); index++) {
      var e = sortedFiles.get(index);
      sum += e.getValue().getEstimatedSize();

      if (sum < cutoff)
        ret.put(e.getKey(), e.getValue());
      else
        break;
    }

    return ret;
  }

  String getExecutor(long size) {
    long meg = 1000000;

    if (size < 10 * meg) {
      return "small";
    } else if (size < 100 * meg) {
      return "medium";
    } else if (size < 500 * meg) {
      return "large";
    } else {
      return "huge";
    }
  }

  public static List<Entry<URI,FileInfo>> sortByFileSize(Map<URI,FileInfo> files) {
    List<Entry<URI,FileInfo>> sortedFiles = new ArrayList<>(files.entrySet());

    // sort from smallest file to largest
    Collections.sort(sortedFiles,
        Comparator.comparingLong((Entry<URI,FileInfo> entry) -> entry.getValue().getEstimatedSize())
            .thenComparing(Entry::getKey));

    return sortedFiles;
  }
}
