/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.compaction;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.UserCompactionUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;

public class CompactionFileSelector {

  private static final Logger log = LoggerFactory.getLogger(CompactionFileSelector.class);

  private static <T> T newInstance(AccumuloConfiguration tableConfig, String className,
      Class<T> baseClass) {
    String context = ClassLoaderUtil.tableContext(tableConfig);
    try {
      return ConfigurationTypeHelper.getClassInstance(context, className, baseClass);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static Set<StoredTabletFile> selectFiles(ServerContext context, KeyExtent extent,
      CompactionConfig compactionConfig, Map<StoredTabletFile,DataFileValue> allFiles) {
    if (!UserCompactionUtils.isDefault(compactionConfig.getSelector())) {
      return selectFiles(context, extent, allFiles, compactionConfig.getSelector());
    } else {
      return allFiles.keySet();
    }
  }

  private static Set<StoredTabletFile> selectFiles(ServerContext context, KeyExtent extent,
      Map<StoredTabletFile,DataFileValue> datafiles, PluginConfig selectorConfig) {

    log.debug("Selecting files for {} using {}", extent, selectorConfig);

    CompactionSelector selector = newInstance(context.getTableConfiguration(extent.tableId()),
        selectorConfig.getClassName(), CompactionSelector.class);

    final ServiceEnvironment senv = new ServiceEnvironmentImpl(context);

    selector.init(new CompactionSelector.InitParameters() {
      @Override
      public Map<String,String> getOptions() {
        return selectorConfig.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return senv;
      }

      @Override
      public TableId getTableId() {
        return extent.tableId();
      }
    });

    CompactionSelector.Selection selection =
        selector.select(new CompactionSelector.SelectionParameters() {
          @Override
          public PluginEnvironment getEnvironment() {
            return senv;
          }

          @Override
          public Collection<CompactableFile> getAvailableFiles() {
            return Collections2.transform(datafiles.entrySet(),
                e -> new CompactableFileImpl(e.getKey(), e.getValue()));
          }

          @Override
          public Collection<Summary> getSummaries(Collection<CompactableFile> files,
              Predicate<SummarizerConfiguration> summarySelector) {

            throw new UnsupportedOperationException();
          }

          @Override
          public TableId getTableId() {
            return extent.tableId();
          }

          @Override
          public TabletId getTabletId() {
            return new TabletIdImpl(extent);
          }

          @Override
          public Optional<SortedKeyValueIterator<Key,Value>> getSample(CompactableFile file,
              SamplerConfiguration sc) {
            throw new UnsupportedOperationException();
          }
        });

    return selection.getFilesToCompact().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());
  }

}
