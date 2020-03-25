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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compactions.Compactable.Files;

public class TieredCompactionPlanner implements CompactionPlanner {

  @Override
  public CompactionPlan makePlan(CompactionType type, Files files, double cRatio) {
    // TODO only create if needed in an elegant way.

    Map<StoredTabletFile,DataFileValue> filesCopy = new HashMap<>(files.allFiles);
    filesCopy.keySet().retainAll(files.candidates);

    // find minimum file size for running compactions
    OptionalLong minCompacting =
        files.compacting.stream().map(files.allFiles::get).mapToLong(DataFileValue::getSize).min();

    if (minCompacting.isPresent()) {
      // This is done to ensure that compactions over time result in the mimimum number of files.
      // See the gist for more info.
      filesCopy = getSmallestFilesWithSumLessThan(filesCopy, minCompacting.getAsLong());
    }

    Set<StoredTabletFile> group = findMapFilesToCompact(filesCopy, cRatio);

    if (group.isEmpty() && (type == CompactionType.USER || type == CompactionType.CHOP)) {
      group = files.candidates;
    }

    if (group.isEmpty()) {
      return new CompactionPlan();
    } else {

      // TODO do we want to queue a job to an executor if we already have something running there??
      // determine which executor to use based on the size of the files
      String executor =
          getExecutor(group.stream().mapToLong(file -> files.allFiles.get(file).getSize()).sum());
      CompactionJob job = new CompactionJob(files.allFiles.size(), executor, group, type);
      return new CompactionPlan(List.of(job));
    }
  }

  /**
   * Find the largest set of small files to compact.
   *
   * <p>
   * See https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
   */
  public static Set<StoredTabletFile>
      findMapFilesToCompact(Map<StoredTabletFile,DataFileValue> files, double ratio) {
    if (files.size() <= 1)
      return Collections.emptySet();

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<Entry<StoredTabletFile,DataFileValue>> sortedFiles = sortByFileSize(files);

    // index into sortedFiles, everything at and below this index is a good set of files to compact
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getValue().getSize();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getValue().getSize();
      sum += currSize;

      if (currSize * ratio < sum) {
        goodIndex = c;

        // look ahead to the next file to see if this a good stopping point
        if (c + 1 < sortedFiles.size()) {
          long nextSize = sortedFiles.get(c + 1).getValue().getSize();
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

    return sortedFiles.subList(0, goodIndex + 1).stream()
        .map(Entry<StoredTabletFile,DataFileValue>::getKey).collect(toSet());
  }

  public static Map<StoredTabletFile,DataFileValue>
      getSmallestFilesWithSumLessThan(Map<StoredTabletFile,DataFileValue> files, long cutoff) {
    List<Entry<StoredTabletFile,DataFileValue>> sortedFiles = sortByFileSize(files);

    HashMap<StoredTabletFile,DataFileValue> ret = new HashMap<>();

    long sum = 0;

    for (int index = 0; index < sortedFiles.size(); index++) {
      var e = sortedFiles.get(index);
      sum += e.getValue().getSize();

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

  public static List<Entry<StoredTabletFile,DataFileValue>>
      sortByFileSize(Map<StoredTabletFile,DataFileValue> files) {
    List<Entry<StoredTabletFile,DataFileValue>> sortedFiles = new ArrayList<>(files.entrySet());

    // sort from smallest file to largest
    Collections.sort(sortedFiles,
        Comparator
            .comparingLong(
                (Entry<StoredTabletFile,DataFileValue> entry) -> entry.getValue().getSize())
            .thenComparing(Entry::getKey));

    return sortedFiles;
  }
}
