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

import java.util.EnumMap;
import java.util.Map;

/**
 *
 * {@code table.compaction.dispatcher.opts.service[.user[.<user type>]|maintenance|chop]=<service>}
 *
 */

public class SimpleCompactionDispatcher implements CompactionDispatcher {

  private Map<CompactionKind,CompactionDirectives> services;
  private Map<String,CompactionDirectives> userServices;

  @Override
  public void init(InitParameters params) {
    // TODO precompute hint types

    services = new EnumMap<>(CompactionKind.class);

    var defaultService = CompactionDirectives.builder().build();

    if (params.getOptions().containsKey("service")) {
      defaultService =
          CompactionDirectives.builder().setService(params.getOptions().get("service")).build();
    }

    for (CompactionKind ctype : CompactionKind.values()) {
      String service = params.getOptions().get("service." + ctype.name().toLowerCase());
      if (service == null)
        services.put(ctype, defaultService);
      else
        services.put(ctype, CompactionDirectives.builder().setService(service).build());
    }

    params.getOptions().forEach((k, v) -> {
      if (k.startsWith("service.user.")) {
        String type = k.substring("service.user.".length());
        userServices.put(type, CompactionDirectives.builder().setService(v).build());
      }
    });
  }

  @Override
  public CompactionDirectives dispatch(DispatchParameters params) {

    if (params.getCompactionKind() == CompactionKind.USER) {
      String hintType = params.getExecutionHints().get("compaction_type");
      if (hintType != null) {
        var userDirectives = userServices.get(hintType);
        if (userDirectives != null) {
          return userDirectives;
        } else {
          // TODO
        }
      }
    }
    return services.get(params.getCompactionKind());
  }

}
