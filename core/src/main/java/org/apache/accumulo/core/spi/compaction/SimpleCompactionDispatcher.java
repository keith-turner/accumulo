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

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.slf4j.Logger;

/**
 * Dispatcher that supports simple configuration for making tables use compaction services. By
 * default it dispatches to a compction service named default.
 *
 * <p>
 * The following schema is supported for configration options.
 *
 * <p>
 * {@code table.compaction.dispatcher.opts.service[.user[.<user type>]|selected|system|chop]=
 * <service>}
 *
 * <p>
 * The following configuration will make a table use compaction service cs9 for user compactions,
 * service cs4 for chop compactions, and service cs7 for everything else.
 *
 * <p>
 * {@code
 *   table.compaction.dispatcher.opts.service=cs7
 *   table.compaction.dispatcher.opts.service.user=cs9
 *   table.compaction.dispatcher.opts.service.chop=cs4
 * }
 *
 * <p>
 * Compactions started using the client API are called user compactions and can set execution hints
 * using {@link CompactionConfig#setExecutionHints(Map)}. Hints of the form
 * {@code compaction_type=<user type>} can be used by this dispatcher. For example the following
 * will use service cs2 when the hint {@code compaction_type=urgent} is seen, service cs3 when hint
 * {@code compaction_type=trifling}, everything else uses cs9.
 *
 * <p>
 * {@code
 *   table.compaction.dispatcher.opts.service=cs9
 *   table.compaction.dispatcher.opts.service.user.urgent=cs2
 *   table.compaction.dispatcher.opts.service.user.trifling=cs3
 * }
 *
 * @see org.apache.accumulo.core.spi.compaction
 */

public class SimpleCompactionDispatcher implements CompactionDispatcher {

  private Map<CompactionKind,CompactionDirectives> services;
  private Map<String,CompactionDirectives> userServices;

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(SimpleCompactionDispatcher.class);

  @Override
  public void init(InitParameters params) {
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

    // TODO remove
    log.debug("services:{} userServices:{}", services, userServices, new Exception());
  }

  @Override
  public CompactionDirectives dispatch(DispatchParameters params) {

    if (params.getCompactionKind() == CompactionKind.USER) {
      String hintType = params.getExecutionHints().get("compaction_type");
      if (hintType != null) {
        var userDirectives = userServices.get(hintType);
        if (userDirectives != null) {
          return userDirectives;
        }
      }
    }
    return services.get(params.getCompactionKind());
  }

}
