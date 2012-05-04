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
package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletServerMutations;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.ScriptBasedMapping;

/**
 * 
 */
class RackUtil {
  
  static class ServerSelector {
    private Connector conn;
    private DNSToSwitchMapping rackMapping;
    private Map<String, List<String>> servers;
    private Random random;
    
    ServerSelector(Connector conn, DNSToSwitchMapping rackMapping){
      this.conn = conn;
      this.rackMapping = rackMapping;
      servers = new HashMap<String,List<String>>();
      random = new Random();
      
      List<String> tservers = conn.instanceOperations().getTabletServers();
      List<String> strippedTservers = new ArrayList<String>(tservers.size());
      
      for (String tserver : tservers) {
        strippedTservers.add(tserver.split(":")[0]);
      }
      
      List<String> racks = rackMapping.resolve(strippedTservers);
      
      for (int i = 0; i < tservers.size(); i++) {
        String server = tservers.get(i);
        String rack = racks.get(i);
        
        List<String> sl = servers.get(rack);
        if(sl == null){
          sl = new ArrayList<String>();
          servers.put(rack, sl);
        }
        
        sl.add(server);
      }
    }
    
    String getRandomSever(String rack){
      List<String> sl = servers.get(rack);
      return sl.get(random.nextInt(sl.size()));
    }
    
  }
  
  
  // take mutations binned by tablet server and bin them by rack
  static Map<String,Map<String,TabletServerMutations>> binToRack(DNSToSwitchMapping rackMapping, ServerSelector serverSelector,
      Map<String,TabletServerMutations> binnedMutations) {
    ArrayList<String> serversList = new ArrayList<String>(binnedMutations.keySet());

    List<String> strippedTservers = new ArrayList<String>(serversList.size());
    
    for (String tserver : serversList) {
      strippedTservers.add(tserver.split(":")[0]);
    }
    
    List<String> racks = rackMapping.resolve(strippedTservers);
    
    // a randomg server is selected for each rack, once a random server is selected for a rack keep using it...
    // the map below is intended to keep track of this
    Map<String,String> rackServers = new HashMap<String,String>();
    
    Map<String,Map<String,TabletServerMutations>> ret = new HashMap<String,Map<String,TabletServerMutations>>();
    
    for (int i = 0; i < serversList.size(); i++) {
      String server = serversList.get(i);
      String rack = racks.get(i);
      
      String rackServer = rackServers.get(rack);
      
      if (rackServer == null) {
        rackServer = serverSelector.getRandomSever(rack);
        rackServers.put(rack, rackServer);
      }
      
      Map<String,TabletServerMutations> rackData = ret.get(rackServer);
      if (rackData == null) {
        rackData = new HashMap<String,TabletLocator.TabletServerMutations>();
        ret.put(rackServer, rackData);
      }
      
      rackData.put(server, binnedMutations.get(server));
    }
    
    return ret;
  }
  
  public static void main(String[] args) throws AccumuloException, AccumuloSecurityException {
    ZooKeeperInstance zki = new ZooKeeperInstance("test15", "localhost");
    Connector conn = zki.getConnector("root", "secret");
    
    DNSToSwitchMapping rm = new ScriptBasedMapping();
    
    System.out.println(rm.resolve(Arrays.asList("127.0.0.1")));
    
    ServerSelector ss = new ServerSelector(conn, rm);
    System.out.println(ss.getRandomSever("/default-rack"));
  }
}
