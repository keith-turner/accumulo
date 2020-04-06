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
package org.apache.accumulo.core.client.admin;

import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.clientImpl.CompactionStrategyConfigUtil.DEFAULT_STRATEGY;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.hadoop.io.Text;

import com.google.common.base.Preconditions;

/**
 * This class exist to pass parameters to {@link TableOperations#compact(String, CompactionConfig)}
 *
 * @since 1.7.0
 */
public class CompactionConfig {

  private Text start = null;
  private Text end = null;
  private boolean flush = true;
  private boolean wait = true;
  private List<IteratorSetting> iterators = Collections.emptyList();
  private CompactionStrategyConfig compactionStrategy = DEFAULT_STRATEGY;
  private Map<String,String> hints;
  private CompactionSelectorConfig selectorConfig = UserCompactionUtils.DEFAULT_CSC;
  private CompactionConfigurerConfig configurerConfig = UserCompactionUtils.DEFAULT_CCC;

  /**
   * @param start
   *          First tablet to be compacted contains the row after this row, null means the first
   *          tablet in table. The default is null.
   * @return this
   */

  public CompactionConfig setStartRow(Text start) {
    this.start = start;
    return this;
  }

  /**
   * @return The previously set start row. The default is null.
   */
  public Text getStartRow() {
    return start;
  }

  /**
   *
   * @param end
   *          Last tablet to be compacted contains this row, null means the last tablet in table.
   *          The default is null.
   * @return this
   */
  public CompactionConfig setEndRow(Text end) {
    this.end = end;
    return this;
  }

  /**
   * @return The previously set end row. The default is null.
   */
  public Text getEndRow() {
    return end;
  }

  /**
   * @param flush
   *          If set to true, will flush in memory data of all tablets in range before compacting.
   *          If not set, the default is true.
   * @return this
   */
  public CompactionConfig setFlush(boolean flush) {
    this.flush = flush;
    return this;
  }

  /**
   * @return The previously set flush. The default is true.
   */
  public boolean getFlush() {
    return flush;
  }

  /**
   * @param wait
   *          If set to true, will cause compact operation to wait for all tablets in range to
   *          compact. If not set, the default is true.
   * @return this
   */

  public CompactionConfig setWait(boolean wait) {
    this.wait = wait;
    return this;
  }

  /**
   *
   * @return The previously set wait. The default is true.
   */
  public boolean getWait() {
    return wait;
  }

  /**
   * @param iterators
   *          configures the iterators that will be used when compacting tablets. These iterators
   *          are merged with current iterators configured for the table.
   * @return this
   */
  public CompactionConfig setIterators(List<IteratorSetting> iterators) {
    this.iterators = List.copyOf(iterators);
    return this;
  }

  /**
   * @return The previously set iterators. Returns an empty list if not set. The returned list is
   *         unmodifiable.
   */
  public List<IteratorSetting> getIterators() {
    return Collections.unmodifiableList(iterators);
  }

  /**
   * @param csConfig
   *          configures the strategy that will be used by each tablet to select files. If no
   *          strategy is set, then all files will be compacted.
   * @return this
   * @deprecated since 2.1.0
   */
  @Deprecated
  public CompactionConfig setCompactionStrategy(CompactionStrategyConfig csConfig) {
    requireNonNull(csConfig);
    Preconditions.checkArgument(!csConfig.getClassName().isBlank());
    Preconditions.checkState(
        selectorConfig.getClassName().isEmpty() && configurerConfig.getClassName().isEmpty());
    this.compactionStrategy = csConfig;
    return this;
  }

  /**
   * @return The previously set compaction strategy. Defaults to a configuration of
   *         org.apache.accumulo.tserver.compaction.EverythingCompactionStrategy which always
   *         compacts all files.
   * @deprecated since 2.1.0
   */
  @Deprecated
  public CompactionStrategyConfig getCompactionStrategy() {
    return compactionStrategy;
  }

  /**
   * @return this;
   * @since 2.1.0
   */
  public CompactionConfig setSelector(CompactionSelectorConfig selectorConfig) {
    Preconditions.checkState(compactionStrategy.getClassName().isEmpty());
    Preconditions.checkArgument(!selectorConfig.getClassName().isBlank());
    this.selectorConfig = requireNonNull(selectorConfig);
    return this;
  }

  /**
   * @since 2.1.0
   */
  public CompactionSelectorConfig getSelector() {
    return selectorConfig;
  }

  /**
   * @since 2.1.0
   */
  public CompactionConfig setExecutionHints(Map<String,String> hints) {
    Preconditions.checkState(compactionStrategy.getClassName().isEmpty());
    this.hints = Map.copyOf(hints);
    return this;
  }

  /**
   * @since 2.1.0
   */
  public Map<String,String> getExecutionHints() {
    return hints;
  }

  /**
   * Set and override any per-table properties that configure compaction file output like
   * compression.
   *
   * @since 2.1.0
   */
  public CompactionConfig setConfigurer(CompactionConfigurerConfig configurerConfig) {
    Preconditions.checkState(compactionStrategy.getClassName().isEmpty());
    this.configurerConfig = configurerConfig;
    return this;
  }

  /**
   * @since 2.1.0
   */
  public CompactionConfigurerConfig getConfigurer() {
    return configurerConfig;
  }
}
