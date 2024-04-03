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
package org.apache.accumulo.server.validation;

import java.util.List;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.validation.table.CompactionDispatcherValidator;
import org.apache.accumulo.server.validation.table.IteratorValidator;
import org.apache.accumulo.server.validation.table.ScanDispatcherValidator;
import org.apache.accumulo.server.validation.table.TableBalancerValidator;

/**
 * Implementations of this class know how to validate a particular type of table level Accumulo
 * plugin.
 */
public interface TablePluginValidator {
  boolean validate(TableId tableId, ServerContext context);

  /**
   * @return a list of all known table level plugin validators
   */
  static List<TablePluginValidator> validators() {
    return List.of(new CompactionDispatcherValidator(), new IteratorValidator(),
        new ScanDispatcherValidator(), new TableBalancerValidator());
  }
}