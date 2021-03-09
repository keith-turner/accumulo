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
package org.apache.accumulo.server.compaction;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.server.ServerContext;

public class ExternalCompactionUtil {

  /**
   * Utility for returning the address of a service in the form host:port
   *
   * @param address
   *          HostAndPort of service
   * @return host and port
   */
  public static String getHostPortString(HostAndPort address) {
    if (address == null) {
      return null;
    }
    return address.getHost() + ":" + address.getPort();
  }

  /**
   * 
   * @param context
   * @return
   */
  public static HostAndPort findCompactionCoordinator(ServerContext context) {
    final String lockPath = context.getZooKeeperRoot() + Constants.ZCOORDINATOR_LOCK;
    byte[] address = context.getZooCache().get(lockPath);
    String coordinatorAddress = new String(address);
    return HostAndPort.fromString(coordinatorAddress);
  }
}
