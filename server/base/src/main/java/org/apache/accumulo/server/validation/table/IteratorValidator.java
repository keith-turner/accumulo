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
package org.apache.accumulo.server.validation.table;

import java.util.TreeMap;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.IteratorBuilder;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.iterators.TabletIteratorEnvironment;
import org.apache.accumulo.server.validation.TablePluginValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to validate iterator plugins configured on table.
 */
public class IteratorValidator implements TablePluginValidator {

  private static final Logger log = LoggerFactory.getLogger(IteratorValidator.class);

  @Override
  public boolean validate(TableId tableId, ServerContext context) {
    try {

      TableConfiguration tableConf = context.getTableConfiguration(tableId);

      for (var scope : IteratorUtil.IteratorScope.values()) {
        // an empty source
        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(new TreeMap<>());
        TableConfiguration.ParsedIteratorConfig pic = tableConf.getParsedIteratorConfig(scope);
        IteratorEnvironment iterEnv;
        if (scope == IteratorUtil.IteratorScope.scan) {
          iterEnv = new TabletIteratorEnvironment(context, scope, tableConf, tableId);
        } else {
          iterEnv = new TabletIteratorEnvironment(context, scope, false, tableConf, tableId,
              CompactionKind.SYSTEM);
        }

        IteratorBuilder iteratorBuilder = IteratorBuilder.builder(pic.getIterInfo())
            .opts(pic.getOpts()).env(iterEnv)
            .useClassLoader(ClassLoaderUtil.tableContext(tableConf)).useClassCache(false).build();

        IteratorConfigUtil.loadIterators(source, iteratorBuilder);
      }
      return true;
    } catch (Exception e) {
      log.warn("Failed to validate iterators for table {}", tableId, e);
      return false;
    }

  }
}
