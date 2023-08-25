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
package org.apache.accumulo.test;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WriteAfterCloseIT extends AccumuloClusterHarness {

  // TODO may needs its own IT because of higher timeout
  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(300);
  }

  public static class SleepyConstraint implements Constraint {

    @Override
    public String getViolationDescription(short violationCode) {
      return "No such violation";
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      // the purpose of this constraint is to just randomly hold up writes on the server side
      SecureRandom rand = new SecureRandom();
      if (rand.nextBoolean()) {
        UtilWaitThread.sleep(4000);
      }

      return null;
    }
  }

  @Test
  public void testWriteAfterClose() throws Exception {
    // re #3721 test that tries to cause a write event to happen after a batch writer is closed
    String table = getUniqueNames(1)[0];
    var props = new Properties();
    props.putAll(getClientProps());
    props.setProperty(Property.GENERAL_RPC_TIMEOUT.getKey(), "1s");

    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(
        Map.of(Property.TABLE_CONSTRAINT_PREFIX.getKey() + "1", SleepyConstraint.class.getName()));

    // The short rpc timeout and the random sleep in the constraint can cause some of the writes
    // done by a batch writer to timeout. The batch writer will internally retry the write, but the
    // timed out write could still go through at a later time.

    var executor = Executors.newCachedThreadPool();

    try (AccumuloClient c = Accumulo.newClient().from(props).build()) {
      c.tableOperations().create(table, ntc);

      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < 100; i++) {
        futures.add(executor.submit(createWriteTask(i * 1000, c, table)));
      }

      // wait for all futures to complete
      for (var future : futures) {
        future.get();
      }

      try (Scanner scanner = c.createScanner(table)) {
        Assertions.assertEquals(0, scanner.stream().count());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static Callable<Void> createWriteTask(int partition, AccumuloClient c, String table) {
    Callable<Void> task = () -> {
      int row = partition;

      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("r" + row);
        m.put("f1", "q1", new Value("v1"));
        writer.addMutation(m);
      }

      // Relying on the internal retries of the batch writer, trying to create a situation where
      // some of the writes from above actually happen after the delete below which would negate the
      // delete.

      try (BatchWriter writer = c.createBatchWriter(table)) {
        Mutation m = new Mutation("r" + row);
        m.putDelete("f1", "q1");
        writer.addMutation(m);
      }
      return null;
    };
    return task;
  }
}
