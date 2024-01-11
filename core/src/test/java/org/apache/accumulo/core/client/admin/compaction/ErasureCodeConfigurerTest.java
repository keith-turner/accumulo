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
package org.apache.accumulo.core.client.admin.compaction;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErasureCodeConfigurerTest {
  @Test
  public void testMinNNOverhead() {
    var ecc = new ErasureCodeConfigurer();

    var tableProps = Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "true",
        Property.TABLE_ERASURE_CODE_POLICY.getKey(), "coder-6-3-?",
        Property.TABLE_FILE_BLOCK_SIZE.getKey(), "64M", Property.TABLE_FILE_REPLICATION.getKey(),
        "4");
    var options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_MIN_NAMENODE_OVERHEAD, "true");

    ecc.init(newInitParams(tableProps, options));
    var overrides = ecc.override(newInputParams("F1", "1M"));

    Assertions.assertEquals("false",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));

    overrides = ecc.override(newInputParams("F1", "2G", "F2", "2G", "F3", "3G", "F4", "3G"));

    Assertions.assertEquals("true",
        overrides.getOverrides().get(Property.TABLE_ENABLE_ERASURE_CODES.getKey()));

  }

  private CompactionConfigurer.InputParameters newInputParams(String... namesSizePairs) {
    return new CompactionConfigurer.InputParameters() {
      @Override
      public TableId getTableId() {
        return TableId.of("42");
      }

      @Override
      public Collection<CompactableFile> getInputFiles() {
        return createCFs(namesSizePairs);
      }

      @Override
      public PluginEnvironment getEnvironment() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static CompactionConfigurer.InitParameters newInitParams(Map<String,String> tableProps,
      Map<String,String> options) {
    return new CompactionConfigurer.InitParameters() {
      @Override
      public TableId getTableId() {
        return TableId.of("42");
      }

      @Override
      public Map<String,String> getOptions() {
        return options;
      }

      @Override
      public PluginEnvironment getEnvironment() {
        return new PluginEnvironment() {
          @Override
          public Configuration getConfiguration() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Configuration getConfiguration(TableId tableId) {
            return new ConfigurationImpl(
                SiteConfiguration.empty().withOverrides(tableProps).build());
          }

          @Override
          public String getTableName(TableId tableId) throws TableNotFoundException {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> T instantiate(String className, Class<T> base) throws Exception {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> T instantiate(TableId tableId, String className, Class<T> base)
              throws Exception {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  // TODO this was copied from DefaultCompactionPlanner test, maybe share code
  private static Set<CompactableFile> createCFs(String... namesSizePairs) {
    Set<CompactableFile> files = new HashSet<>();

    for (int i = 0; i < namesSizePairs.length; i += 2) {
      String name = namesSizePairs[i];
      long size = ConfigurationTypeHelper.getFixedMemoryAsBytes(namesSizePairs[i + 1]);
      try {
        files.add(CompactableFile
            .create(new URI("hdfs://fake/accumulo/tables/1/t-0000000z/" + name + ".rf"), size, 0));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    return files;
  }
}
