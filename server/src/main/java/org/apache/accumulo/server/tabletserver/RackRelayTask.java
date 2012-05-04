/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.tabletserver;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.thrift.TServiceClient;

/**
 * 
 */
public class RackRelayTask implements Callable<UpdateErrors> {
  
  private LinkedBlockingQueue<Pair<TKeyExtent,List<TMutation>>> queue = new LinkedBlockingQueue<Pair<TKeyExtent,List<TMutation>>>();
  private String location;
  private AuthInfo credentials;
  
  RackRelayTask(String location, AuthInfo credentials) {
    this.location = location;
    this.credentials = credentials;
  }
  
  @Override
  public UpdateErrors call() throws Exception {
    // TODO which config to use?
    TabletClientService.Iface client = ThriftUtil.getTServerClient(location, ServerConfiguration.getSiteConfiguration());
    try {
      long uid = client.startUpdate(null, credentials);
      // TODO if client stops sending stuff, this will wait around forever... need to cancel task when session goes out of scope
      Pair<TKeyExtent,List<TMutation>> pair = queue.take();
      while (pair.getFirst() != null) {
        client.applyUpdates(null, uid, pair.getFirst(), pair.getSecond());
        pair = queue.take();
      }
      
      return client.closeUpdate(null, uid);
    } finally {
      ThriftUtil.returnClient((TServiceClient) client);
    }
  }
  
  public void addMutations(TKeyExtent extent, List<TMutation> mutations) {
    // TODO block if too much mem used? Or should the blocking be at a higher level?
    queue.add(new Pair<TKeyExtent,List<TMutation>>(extent, mutations));
  }
  
  public void close() {
    queue.add(new Pair<TKeyExtent,List<TMutation>>(null, null));
  }

  
}
