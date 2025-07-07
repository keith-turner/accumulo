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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlannerTest;
import org.junit.jupiter.api.Test;

public class ErasureCodeConfigurerTest {

  // TODO need to test file size limit

  @Test
  public void testSize() {
    var ecc = new ErasureCodeConfigurer();

    var options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "1G");
    ecc.init(newInitParams(options));

    var overrides = ecc.override(newInputParams("F1", "1M"));
    assertEquals(Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "false"),
        overrides.getOverrides());

    overrides = ecc.override(newInputParams("F1", "2G", "F2", "2G", "F3", "3G", "F4", "3G"));
    assertEquals(Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "true"),
        overrides.getOverrides());

    options = Map.of(ErasureCodeConfigurer.ERASURE_CODE_SIZE, "1G",
        ErasureCodeConfigurer.ERASURE_CODE_POLICY, "pol",
        CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, "1G",
        CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "bz");
    ecc = new ErasureCodeConfigurer();
    ecc.init(newInitParams(options));

    overrides = ecc.override(newInputParams("F1", "1M"));
    assertEquals(Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "false"),
        overrides.getOverrides());

    overrides = ecc.override(newInputParams("F1", "2G", "F2", "2G", "F3", "3G", "F4", "3G"));
    assertEquals(Map.of(Property.TABLE_ENABLE_ERASURE_CODES.getKey(), "true",
        Property.TABLE_ERASURE_CODE_POLICY.getKey(), "pol",
        Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "bz"), overrides.getOverrides());
  }

  private CompactionConfigurer.InputParameters newInputParams(String... namesSizePairs) {
    return new CompactionConfigurer.InputParameters() {
      @Override
      public TableId getTableId() {
        return TableId.of("42");
      }

      @Override
      public Collection<CompactableFile> getInputFiles() {
        return DefaultCompactionPlannerTest.createCFs(namesSizePairs);
      }

      @Override
      public PluginEnvironment getEnvironment() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static CompactionConfigurer.InitParameters newInitParams(Map<String,String> options) {
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
        throw new UnsupportedOperationException();
      }
    };
  }

  // TODO need to test invalid config and ensure exception is thrown
}
