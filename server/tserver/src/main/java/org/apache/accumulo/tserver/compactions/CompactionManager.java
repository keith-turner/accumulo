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
package org.apache.accumulo.tserver.compactions;

import java.util.Map;

import org.apache.accumulo.tserver.compactions.CompactionService.Id;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionManager {

  private static final Logger log = LoggerFactory.getLogger(CompactionManager.class);

  private Iterable<Compactable> compactables;
  private Map<CompactionService.Id,CompactionService> services;

  private static Text getEndrow(Compactable c) {
    return c.getExtent().getEndRow();
  }

  /*
   * private synchronized void printStats() { try {
   * 
   * Map<String,Integer> epos = Map.of("small", 1, "medium", 3, "large", 5, "huge", 7);
   * 
   * String[] columns = {"not compacting", "small running", "small queued", "medium running",
   * "medium queued", "large running", "large queued", "huge running", "huge queued"};
   * 
   * while (true) {
   * 
   * List<Compactable> sortedCompactables = new ArrayList<Compactable>();
   * compactables.forEach(sortedCompactables::add); sortedCompactables.removeIf( c ->
   * c.getExtent().isMeta() || c.getExtent().getTableId().canonical().equals("+rep") ||
   * c.getExtent().getTableId().canonical().equals("1")); Collections.sort(sortedCompactables,
   * Comparator.comparing(CompactionManager::getEndrow,
   * Comparator.nullsLast(Comparator.naturalOrder())));
   * 
   * int[][] data = new int[sortedCompactables.size()][]; String[] rows = new
   * String[sortedCompactables.size()];
   * 
   * for (int i = 0; i < sortedCompactables.size(); i++) { int r = i; var compactable =
   * sortedCompactables.get(r);
   * 
   * rows[r] = compactable.getExtent().getEndRow() + "";
   * 
   * data[r] = new int[columns.length];
   * 
   * submittedJobs.row(compactable.getExtent()).values().forEach(sjob -> { var status =
   * sjob.getStatus();
   * 
   * if (status == Status.QUEUED || status == Status.RUNNING) { int pos =
   * epos.get(sjob.getJob().getExecutor()); if (status == Status.QUEUED) pos++; data[r][pos] =
   * sjob.getJob().getFiles().size(); } });
   * 
   * data[r][0] = compactable.getFiles().size(); }
   * 
   * if (rows.length > 0) { System.out.println("Compaction stats : " + new Date());
   * System.out.println(new PrintableTable(columns, rows, data).toString()); }
   * 
   * try { wait(1000); } catch (InterruptedException e) { // TODO Auto-generated catch block
   * e.printStackTrace(); } } } catch (Exception e) { log.error("CMSF", e); } }
   */
  // TODO remove sync... its a hack for printStats
  private synchronized void mainLoop() {
    while (true) {
      try {

        for (Compactable compactable : compactables) {
          var service = services.get(compactable.getConfiguredService(CompactionType.MAINTENANCE));
          service.compact(CompactionType.MAINTENANCE, compactable);
        }
      } catch (Exception e) {
        // TODO
        log.error("Loop failed ", e);
      }

      try {
        // TODO use sleep.. using wait because of sync
        wait(3000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // UtilWaitThread.sleep(3000);// TODO configurable
    }
  }

  public CompactionManager(Iterable<Compactable> compactables) {
    this.compactables = compactables;
    this.services = Map.of(CompactionService.Id.of("default"), new CompactionServiceImpl());
  }

  public void start() {
    // TODO deamon thread
    // TODO stop method
    log.info("Started compaction manager");
    new Thread(() -> mainLoop()).start();

    // TODO remove
    // new Thread(() -> printStats()).start();
  }
}
