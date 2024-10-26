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
package org.apache.accumulo.core.spi.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;

public class Test {

  public static void main(String[] args) throws Exception {
    LoggingRegistryConfig lconf = c -> {
      if (c.equals("logging.step")) {
        return "1s";
      }
      return null;
    };

    var compositeRegistry = new CompositeMeterRegistry();
    var loggingRegistry =
        LoggingMeterRegistry.builder(lconf).loggingSink(System.out::println).build();
    loggingRegistry.config().meterFilter(MeterFilter.ignoreTags("tableId"));
    compositeRegistry.add(loggingRegistry);

    var meter1 = DistributionSummary.builder("scan.results").tag("address", "localhost:1234")
        .tag("tableId", "2").register(compositeRegistry);
    var meter2 = DistributionSummary.builder("scan.results").tag("address", "localhost:1234")
        .tag("tableId", "3").register(compositeRegistry);
    var meter3 = DistributionSummary.builder("scan.results").tag("address", "localhost:6789")
            .tag("tableId", "3").register(compositeRegistry);

    System.out.println("loggingRegistry meters   : ");
    loggingRegistry.getMeters().forEach(meter -> System.out.println("  " + meter.getId()));

    System.out.println("compositeRegistry meters : ");
    compositeRegistry.getMeters().forEach(meter -> System.out.println("  " + meter.getId()));

    meter1.record(5);
    meter2.record(10);
    meter3.record(15);
    // let the logger run
    Thread.sleep(2000);

    var removedMeter = compositeRegistry.remove(meter2);
    System.out.println("removed " + removedMeter.getId() + " from compositeRegistry");

    System.out.println("loggingRegistry meters   : ");
    loggingRegistry.getMeters().forEach(meter -> System.out.println("  " + meter.getId()));

    System.out.println("compositeRegistry meters : ");
    compositeRegistry.getMeters().forEach(meter -> System.out.println("  " + meter.getId()));

    meter1.record(6);
    meter3.record(8);
    // give the loggingRegistry a chance to run
    Thread.sleep(5000);
  }
}
