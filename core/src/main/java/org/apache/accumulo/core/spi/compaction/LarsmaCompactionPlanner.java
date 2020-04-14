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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

/**
 * Finds the largest continuous set of small files that meet the compaction ratio and do not prevent
 * future compactions.
 *
 * @since 2.1.0
 * @see org.apache.accumulo.core.spi.compaction
 */

public class LarsmaCompactionPlanner implements CompactionPlanner {

  private static Logger log = LoggerFactory.getLogger(LarsmaCompactionPlanner.class);

  public static class ExecutorConfig {
    String name;
    String maxSize;
    int numThreads;
  }

  private static class Executor {
    final CompactionExecutorId ceid;
    final Long maxSize;

    public Executor(CompactionExecutorId ceid, Long maxSize) {
      Preconditions.checkArgument(maxSize == null || maxSize > 0);
      this.ceid = Objects.requireNonNull(ceid);
      this.maxSize = maxSize;
    }

    Long getMaxSize() {
      return maxSize;
    }
  }

  private List<Executor> executors;
  private int maxFilesToCompact;

  @Override
  public void init(InitParameters params) {
    ExecutorConfig[] execConfigs =
        new Gson().fromJson(params.getOptions().get("executors"), ExecutorConfig[].class);

    List<Executor> tmpExec = new ArrayList<>();

    for (ExecutorConfig executorConfig : execConfigs) {
      var ceid = params.getExecutorManager().createExecutor(executorConfig.name,
          executorConfig.numThreads);
      Long maxSize = executorConfig.maxSize == null ? null
          : ConfigurationTypeHelper.getFixedMemoryAsBytes(executorConfig.maxSize);
      tmpExec.add(new Executor(ceid, maxSize));
    }

    Collections.sort(tmpExec, Comparator.comparing(Executor::getMaxSize,
        Comparator.nullsLast(Comparator.naturalOrder())));

    executors = List.copyOf(tmpExec);

    if (executors.stream().filter(e -> e.getMaxSize() == null).count() > 1) {
      throw new IllegalArgumentException(
          "Can only have one executor w/o a maxSize. " + params.getOptions().get("executors"));
    }

    String fqo = params.getFullyQualifiedOption("maxFilesPerCompaction");

    if (!params.getServiceEnvironment().getConfiguration().isSet(fqo)
        && params.getServiceEnvironment().getConfiguration()
            .isSet(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey())) {
      log.warn("The property " + Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()
          + " was set, it is deperecated.  Set the " + fqo + " option instead.");
      this.maxFilesToCompact =
          this.maxFilesToCompact = Integer.parseInt(params.getServiceEnvironment()
              .getConfiguration().get(Property.TSERV_MAJC_THREAD_MAXOPEN.getKey()));
    } else {
      this.maxFilesToCompact =
          Integer.parseInt(params.getOptions().getOrDefault("maxFilesPerCompaction", "30"));
    }
  }

  @Override
  public CompactionPlan makePlan(PlanningParameters params) {
    try {

      if (params.getCandidates().isEmpty()) {
        // TODO should not even be called in this case
        return new CompactionPlan();
      }

      Set<CompactableFile> filesCopy = new HashSet<>(params.getCandidates());

      long maxSizeToCompact = getMaxSizeToCompact(params.getKind());

      Collection<CompactableFile> group;
      if (params.getRunningCompactions().isEmpty()) {
        group = findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
            maxSizeToCompact);
      } else {
        // This code determines if once the files compacting finish would they be included in a
        // compaction with the files smaller than them? If so, then wait for the running compaction
        // to complete.

        // The set of files running compactions may produce
        var expectedFiles = getExpected(params.getRunningCompactions());

        if (!Collections.disjoint(filesCopy, expectedFiles)) {
          throw new AssertionError();
        }

        filesCopy.addAll(expectedFiles);

        group = findMapFilesToCompact(filesCopy, params.getRatio(), maxFilesToCompact,
            maxSizeToCompact);

        if (!Collections.disjoint(group, expectedFiles)) {
          // file produced by running compaction will eventually compact with existing files, so
          // wait.
          group = Set.of();
        }
      }

      if (group.isEmpty() && params.getKind() == CompactionKind.USER
          && params.getRunningCompactions().stream()
              .filter(job -> job.getKind() == CompactionKind.USER).count() == 0) {
        // TODO consider max files to compact
        group = params.getCandidates();
      }

      if (group.isEmpty() && params.getKind() == CompactionKind.CHOP) {
        // TODO partition files using maxFilesToCompact, executors max sizes, and/or compaction
        // ratio... user and chop could be partitioned differently
        group = params.getCandidates();
      }

      if (group.isEmpty()) {
        return new CompactionPlan();
      } else {
        // determine which executor to use based on the size of the files
        var ceid = getExecutor(group.stream().mapToLong(CompactableFile::getEstimatedSize).sum());

        if (!params.getRunningCompactions().isEmpty()) {
          // TODO remove
          log.info("Planning concurrent {} {}", ceid, group);
        }

        // TODO include type in priority!
        CompactionJob job =
            new CompactionJob(params.getAll().size(), ceid, group, params.getKind());
        return new CompactionPlan(List.of(job));
      }

    } catch (RuntimeException e) {
      // TODO remove
      log.warn("params:{}", params, e);
      throw e;
    }
  }

  private long getMaxSizeToCompact(CompactionKind kind) {
    if (kind == CompactionKind.SYSTEM) {
      Long max = executors.get(executors.size() - 1).maxSize;
      if (max == null)
        max = Long.MAX_VALUE;
    }
    return Long.MAX_VALUE;
  }

  /**
   * @return the expected files sizes for sets of compacting files.
   */
  private Set<CompactableFile> getExpected(Collection<CompactionJob> compacting) {

    Set<CompactableFile> expected = new HashSet<>();

    int count = 0;

    for (CompactionJob job : compacting) {
      count++;
      long size = job.getFiles().stream().mapToLong(CompactableFile::getEstimatedSize).sum();
      try {
        expected.add(CompactableFile.create(
            new URI("hdfs://fake/accumulo/tables/adef/t-zzFAKEzz/FAKE-0000" + count + ".rf"), size,
            0));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }

    }

    return expected;
  }

  /**
   * Find the largest set of small files to compact.
   *
   * <p>
   * See https://gist.github.com/keith-turner/16125790c6ff0d86c67795a08d2c057f
   */
  public static Collection<CompactableFile> findMapFilesToCompact(Set<CompactableFile> files,
      double ratio, int maxFilesToCompact, long maxSizeToCompact) {
    if (files.size() <= 1)
      return Collections.emptySet();

    // sort files from smallest to largest. So position 0 has the smallest file.
    List<CompactableFile> sortedFiles = sortByFileSize(files);

    int larsmaIndex = -1;
    long larsmaSum = Long.MIN_VALUE;

    // index into sortedFiles, everything at and below this index meets the compaction ratio
    int goodIndex = -1;

    long sum = sortedFiles.get(0).getEstimatedSize();

    for (int c = 1; c < sortedFiles.size(); c++) {
      long currSize = sortedFiles.get(c).getEstimatedSize();
      sum += currSize;

      if (sum > maxSizeToCompact)
        break;

      if (currSize * ratio < sum) {
        goodIndex = c;

        if (goodIndex + 1 >= maxFilesToCompact)
          break; // TODO old algorithm used to slide a window up when nothing found in smallest
                 // files
      } else if (c - 1 == goodIndex) {
        // The previous file met the compaction ratio, but the current file does not. So all of the
        // previous files are candidates. However we must ensure that any candidate set produces a
        // file smaller than the next largest file in the next candidate set.
        if (larsmaIndex == -1 || larsmaSum > sortedFiles.get(goodIndex).getEstimatedSize()) {
          larsmaIndex = goodIndex;
          larsmaSum = sum - currSize;
        } else {
          break;
        }
      }
    }

    if (sortedFiles.size() - 1 == goodIndex
        && (larsmaIndex == -1 || larsmaSum > sortedFiles.get(goodIndex).getEstimatedSize())) {
      larsmaIndex = goodIndex;
    }

    if (larsmaIndex == -1)
      return Collections.emptySet();

    return sortedFiles.subList(0, larsmaIndex + 1);
  }

  CompactionExecutorId getExecutor(long size) {
    for (Executor executor : executors) {
      if (size < executor.maxSize)
        return executor.ceid;
    }

    // TODO is this best behavior? Could not compact when there is no executor to service that size
    return executors.get(executors.size() - 1).ceid;
  }

  public static List<CompactableFile> sortByFileSize(Collection<CompactableFile> files) {
    List<CompactableFile> sortedFiles = new ArrayList<>(files);

    // sort from smallest file to largest
    Collections.sort(sortedFiles, Comparator.comparingLong(CompactableFile::getEstimatedSize)
        .thenComparing(CompactableFile::getUri));

    return sortedFiles;
  }
}
