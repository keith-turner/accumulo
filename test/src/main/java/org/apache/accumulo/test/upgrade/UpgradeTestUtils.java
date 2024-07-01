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
package org.apache.accumulo.test.upgrade;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UpgradeTestUtils {
  public static final String BASE_DIR = "/target/upgrade-tests/";
  public static final String ROOT_PASSWORD = "979d55ae-98fd-4d22-9b1c-2d5723546a5e";

  public static File getTestDir(String version, String testName) {
    return new File(System.getProperty("user.dir") + BASE_DIR + testName + "/" + version);
  }

  /**
   * Finds all the dirs for a given test that exists for different versions.
   */
  public static List<File> findTestDirs(String testName) {
    var testRoot = new File(System.getProperty("user.dir") + BASE_DIR + testName);
    var files = testRoot.listFiles();
    if (files == null) {
      return List.of();
    }
    return Arrays.stream(files).filter(File::isDirectory).collect(Collectors.toList());
  }
}
