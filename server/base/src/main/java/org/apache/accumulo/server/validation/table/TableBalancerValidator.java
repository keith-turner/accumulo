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

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.manager.balancer.AssignmentParamsImpl;
import org.apache.accumulo.core.manager.balancer.TServerStatusImpl;
import org.apache.accumulo.core.manager.balancer.TabletServerIdImpl;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.spi.balancer.BalancerEnvironment;
import org.apache.accumulo.core.spi.balancer.SimpleLoadBalancer;
import org.apache.accumulo.core.spi.balancer.TabletBalancer;
import org.apache.accumulo.core.spi.balancer.data.TServerStatus;
import org.apache.accumulo.core.spi.balancer.data.TabletServerId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.balancer.BalancerEnvironmentImpl;
import org.apache.accumulo.server.validation.TablePluginValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableBalancerValidator implements TablePluginValidator {

  private static final Logger log = LoggerFactory.getLogger(TableBalancerValidator.class);

  @Override
  public boolean validate(TableId tableId, ServerContext context) {
    try {
      // Create the system level balancer and then try to use it for this table
      var localTabletBalancer = Property.createInstanceFromPropertyName(context.getConfiguration(),
          Property.MANAGER_TABLET_BALANCER, TabletBalancer.class, new SimpleLoadBalancer());
      BalancerEnvironment balancerEnvironment = new BalancerEnvironmentImpl(context);
      localTabletBalancer.init(balancerEnvironment);

      // To exercise this table, lets simulate an assignment for the table. This may or may not
      // cause
      // the system balancer to create a per table balancer, it depends on what system balancer is
      // configured.
      Map<TabletId,TabletServerId> assignmentsOut = new HashMap<>();

      TabletServerId tserver1 = new TabletServerIdImpl("localhost", 9997, "0123456789");
      TServerStatus status1 = new TServerStatusImpl(new TabletServerStatus());

      // simulate a single tserver to assign to
      SortedMap<TabletServerId,TServerStatus> currentStatus = new TreeMap<>();
      currentStatus.put(tserver1, status1);

      // simulate an unassigned tablet for this tserver
      Map<TabletId,TabletServerId> unassigned =
          Map.of(new TabletIdImpl(new KeyExtent(tableId, null, null)), null);

      TabletBalancer.AssignmentParameters assignParams =
          new AssignmentParamsImpl(currentStatus, unassigned, assignmentsOut);

      // call the balancer and ensure it does not throw an error
      localTabletBalancer.getAssignments(assignParams);
      return true;
    } catch (Exception e) {
      log.warn("Failed to validate balancer for table {}", tableId, e);
      return false;
    }
  }
}
