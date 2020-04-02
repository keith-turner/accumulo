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
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.tserver.compactions.Compactable.Files;
import org.apache.accumulo.tserver.compactions.CompactionServiceImpl.ServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TieredCompactionPlanner implements CompactionPlanner {

  private static Logger log = LoggerFactory.getLogger(TieredCompactionPlanner.class);

  private static class Executor {
    final String name;
    final Long maxSize;

    public Executor(String name, Long maxSize) {
      Preconditions.checkArgument(maxSize == null || maxSize > 0);
      this.name = Objects.requireNonNull(name);
      this.maxSize = maxSize;
    }

    Long getMaxSize() {
      return maxSize;
    }
  }

  private final List<Executor> executors;

  public TieredCompactionPlanner(ServiceConfig serviceConfig) {

    List<Executor> tmpExec = new ArrayList<>();

    serviceConfig.executors.forEach(execCfg -> {
      tmpExec.add(new Executor(execCfg.name, execCfg.maxSize == null ? null
          : ConfigurationTypeHelper.getMemoryAsBytes(execCfg.maxSize)));
    });

    Collections.sort(tmpExec, Comparator.comparing(Executor::getMaxSize,
        Comparator.nullsLast(Comparator.naturalOrder())));

    executors = List.copyOf(tmpExec);
  }

  @Override
  public CompactionPlan makePlan(CompactionType type, Files files, double cRatio) {
    var plan = _makePlan(type, files, cRatio);

    long size = plan.getJobs().stream().flatMap(job -> job.getFiles().stream())
        .map(files.allFiles::get).mapToLong(DataFileValue::getSize).sum();
    // TODO remove?
    log.debug("makePlan({} {} {} -> {} {}", type, files, cRatio, size, plan);
    return plan;
  }

  private CompactionPlan _makePlan(CompactionType type, Files files, double cRatio) {
    try {
      // TODO Property.TSERV_MAJC_THREAD_MAXOPEN

      // TODO only create if needed in an elegant way.

      Map<StoredTabletFile,DataFileValue> filesCopy = new HashMap<>(files.allFiles);
      filesCopy.keySet().retainAll(files.candidates);

      Set<StoredTabletFile> group;
      if (files.compacting.isEmpty()) {
        group = findMapFilesToCompact(filesCopy, cRatio);
      } else {
        // This code determines if once the files compacting finish would they be included in a
        // compaction with the files smaller than them? If so, then wait for the running compaction
        // to complete.

        // The set of files running compactions may produce
        Map<StoredTabletFile,DataFileValue> expectedFiles = getExpected(files);

        if (!Collections.disjoint(filesCopy.keySet(), expectedFiles.keySet())) {
          throw new AssertionError();
        }

        filesCopy.putAll(expectedFiles);

        group = findMapFilesToCompact(filesCopy, cRatio);

        if (!Collections.disjoint(group, expectedFiles.keySet())) {
          // file produced by running compaction will compact with existing files, so wait.
          group = Set.of();
        }
      }

      if (group.isEmpty() && (type == CompactionType.USER || type == CompactionType.CHOP)) {
        group = files.candidates;
      }

      if (group.isEmpty()) {
        return new CompactionPlan();
      } else {

        // TODO do we want to queue a job to an executor if we already have something running
        // there??
        // determine which executor to use based on the size of the files
        String executor =
            getExecutor(group.stream().mapToLong(file -> files.allFiles.get(file).getSize()).sum());

        if (!files.compacting.isEmpty()) {
          // TODO remove
          log.info("Planning concurrent {} {}", executor, group);
        }

        // TODO include type in priority!
        CompactionJob job = new CompactionJob(files.allFiles.size(), executor, group, type);
        return new CompactionPlan(List.of(job));
      }

    } catch (RuntimeException e) {
      log.warn(" type:{} files:{} cRatio:{}", type, files, cRatio, e);
      throw e;
    }
  }

  private Map<StoredTabletFile,DataFileValue> getExpected(Files files) {
    // TODO need to know of sets of compacting files
    if (files.compacting.isEmpty())
      return Map.of();
    StoredTabletFile stf =
        new StoredTabletFile("hdfs://fake/accumulo/tables/adef/t-zzFAKEzz/FAKE-00001.rf");
    DataFileValue newDfv = files.compacting.stream().map(files.allFiles::get)
        .reduce((dfv1, dfv2) -> new DataFileValue(dfv1.getSize() + dfv2.getSize(),
            dfv1.getNumEntries() + dfv2.getNumEntries()))
        .get();

    log.info("Expected {} -> {}", files, newDfv);

    return Map.of(stf, newDfv);
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

  String getExecutor(long size) {
    for (Executor executor : executors) {
      if (size < executor.maxSize)
        return executor.name;
    }

    // TODO is this best behavior? Could not compact when there is no executor to service that size
    return executors.get(executors.size() - 1).name;
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
