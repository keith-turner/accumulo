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
package org.apache.accumulo.tserver.tablet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.admin.CompactionConfigurerConfig;
import org.apache.accumulo.core.client.admin.CompactionSelectorConfig;
import org.apache.accumulo.core.client.admin.CompactionStrategyConfig;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.Selector;
import org.apache.accumulo.core.client.admin.compaction.Selector.Selection;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration.Deriver;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServiceEnvironmentImpl;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.accumulo.tserver.compaction.WriteParameters;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;

public class CompactableUtils {

  public static Map<StoredTabletFile,Pair<Key,Key>> getFirstAndLastKeys(Tablet tablet,
      Set<StoredTabletFile> allFiles) throws IOException {
    final Map<StoredTabletFile,Pair<Key,Key>> result = new HashMap<>();
    final FileOperations fileFactory = FileOperations.getInstance();
    final VolumeManager fs = tablet.getTabletServer().getFileSystem();
    for (StoredTabletFile file : allFiles) {
      FileSystem ns = fs.getFileSystemByPath(file.getPath());
      try (FileSKVIterator openReader = fileFactory.newReaderBuilder()
          .forFile(file.getPathStr(), ns, ns.getConf(), tablet.getContext().getCryptoService())
          .withTableConfiguration(tablet.getTableConfiguration()).seekToBeginning().build()) {
        Key first = openReader.getFirstKey();
        Key last = openReader.getLastKey();
        result.put(file, new Pair<>(first, last));
      }
    }
    return result;
  }

  public static Set<StoredTabletFile> findChopFiles(KeyExtent extent,
      Map<StoredTabletFile,Pair<Key,Key>> firstAndLastKeys, Collection<StoredTabletFile> allFiles) {
    Set<StoredTabletFile> result = new HashSet<>();

    for (StoredTabletFile file : allFiles) {
      Pair<Key,Key> pair = firstAndLastKeys.get(file);
      Key first = pair.getFirst();
      Key last = pair.getSecond();
      // If first and last are null, it's an empty file. Add it to the compact set so it goes
      // away.
      if ((first == null && last == null) || (first != null && !extent.contains(first.getRow()))
          || (last != null && !extent.contains(last.getRow()))) {
        result.add(file);
      }

    }
    return result;
  }

  static CompactionPlan selectFiles(Tablet tablet,
      SortedMap<StoredTabletFile,DataFileValue> datafiles, CompactionStrategyConfig csc) {

    var trsm = tablet.getTabletResources().getTabletServerResourceManager();

    BlockCache sc = trsm.getSummaryCache();
    BlockCache ic = trsm.getIndexCache();
    Cache<String,Long> fileLenCache = trsm.getFileLenCache();
    MajorCompactionRequest request = new MajorCompactionRequest(tablet.getExtent(),
        MajorCompactionReason.USER, tablet.getTabletServer().getFileSystem(),
        tablet.getTableConfiguration(), sc, ic, fileLenCache, tablet.getContext());

    request.setFiles(datafiles);

    // TODO switch compaction strat from using StoredTabletFile
    CompactionStrategy strategy = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        csc.getClassName(), CompactionStrategy.class);
    strategy.init(csc.getOptions());

    try {
      if (strategy.shouldCompact(request)) {
        strategy.gatherInformation(request);
        var plan = strategy.getCompactionPlan(request);
        if (!plan.deleteFiles.isEmpty()) {
          // TODO support
          throw new UnsupportedOperationException(
              "Dropping files using compaction strategy is not supported " + csc);
        }

        return plan;
      }
    } catch (IOException e) {
      // TODO ensure this is handled well
      throw new UncheckedIOException(e);
    }
    return new CompactionPlan();
  }

  static AccumuloConfiguration createCompactionConfiguration(AccumuloConfiguration base,
      WriteParameters p) {
    if (p == null)
      return base;

    ConfigurationCopy result = new ConfigurationCopy(base);
    if (p.getHdfsBlockSize() > 0) {
      result.set(Property.TABLE_FILE_BLOCK_SIZE, "" + p.getHdfsBlockSize());
    }
    if (p.getBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE, "" + p.getBlockSize());
    }
    if (p.getIndexBlockSize() > 0) {
      result.set(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX, "" + p.getIndexBlockSize());
    }
    if (p.getCompressType() != null) {
      result.set(Property.TABLE_FILE_COMPRESSION_TYPE, p.getCompressType());
    }
    if (p.getReplication() != 0) {
      result.set(Property.TABLE_FILE_REPLICATION, "" + p.getReplication());
    }
    return result;
  }

  static AccumuloConfiguration createCompactionConfiguration(Tablet tablet,
      Set<CompactableFile> files, CompactionConfigurerConfig cfg) {
    CompactionConfigurer configurer = CompactableUtils.newInstance(tablet.getTableConfiguration(),
        cfg.getClassName(), CompactionConfigurer.class);

    configurer.init(new CompactionConfigurer.InitParamaters() {
      @Override
      public Map<String,String> getOptions() {
        return cfg.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().getTableId();
      }
    });

    var overrides = configurer.override(new CompactionConfigurer.InputParameters() {
      @Override
      public Collection<CompactableFile> getInputFiles() {
        return files;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public TableId getTableId() {
        return tablet.getExtent().getTableId();
      }
    });

    if (overrides.getOverrides().isEmpty()) {
      return tablet.getTableConfiguration();
    }

    ConfigurationCopy result = new ConfigurationCopy(tablet.getTableConfiguration());
    overrides.getOverrides().forEach(result::set);
    return result;
  }

  static <T> T newInstance(AccumuloConfiguration tableConfig, String className,
      Class<T> baseClass) {
    String context = tableConfig.get(Property.TABLE_CLASSPATH);
    try {
      return ConfigurationTypeHelper.getClassInstance(context, className, baseClass);
    } catch (IOException | ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  static Set<StoredTabletFile> selectFiles(Tablet tablet,
      SortedMap<StoredTabletFile,DataFileValue> datafiles,
      CompactionSelectorConfig selectorConfig) {

    // TODO how are exceptions handled
    Selector selector =
        newInstance(tablet.getTableConfiguration(), selectorConfig.getClassName(), Selector.class);
    selector.init(new Selector.InitParamaters() {

      @Override
      public Map<String,String> getOptions() {
        return selectorConfig.getOptions();
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }
    });

    Selection selection = selector.select(new Selector.SelectionParameters() {

      @Override
      public PluginEnvironment getEnvironment() {
        return new ServiceEnvironmentImpl(tablet.getContext());
      }

      @Override
      public Collection<CompactableFile> getAvailableFiles() {
        // TODO Auto-generated method stub
        return Collections2.transform(datafiles.entrySet(),
            e -> new CompactableFileImpl(e.getKey(), e.getValue()));
      }
    });

    return selection.getFilesToCompact().stream().map(CompactableFileImpl::toStoredTabletFile)
        .collect(Collectors.toSet());
  }

  static Deriver<CompactionDispatcher> createDispatcher(Tablet tablet) {
    return tablet.getTableConfiguration().newDeriver(conf -> {
      CompactionDispatcher newDispatcher = Property.createTableInstanceFromPropertyName(conf,
          Property.TABLE_COMPACTION_DISPATCHER, CompactionDispatcher.class, null);

      var builder = ImmutableMap.<String,String>builder();
      conf.getAllPropertiesWithPrefix(Property.TABLE_COMPACTION_DISPATCHER_OPTS).forEach((k, v) -> {
        String optKey = k.substring(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey().length());
        builder.put(optKey, v);
      });

      Map<String,String> opts = builder.build();

      newDispatcher.init(new CompactionDispatcher.InitParameters() {
        @Override
        public TableId getTableId() {
          return tablet.getExtent().getTableId();
        }

        @Override
        public Map<String,String> getOptions() {
          return opts;
        }

        @Override
        public ServiceEnvironment getServiceEnv() {
          return new ServiceEnvironmentImpl(tablet.getContext());
        }
      });

      return newDispatcher;

    });
  }
}
