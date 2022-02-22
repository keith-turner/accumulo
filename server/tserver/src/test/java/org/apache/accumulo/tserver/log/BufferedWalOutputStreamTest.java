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
package org.apache.accumulo.tserver.log;

import org.apache.hadoop.fs.Syncable;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;

public class BufferedWalOutputStreamTest {
  private static class TestOS extends ByteArrayOutputStream implements Syncable {

    @Override public void hflush() throws IOException {
      flush();
    }

    @Override public void hsync() throws IOException {
      flush();
    }
  }

  @Test
  public void test1() throws IOException {
    Random rand = new Random();

    byte[] data = new byte[10000000];

    for(int i = 0; i<100;i++) {
      int position = 0;

      rand.nextBytes(data);

      TestOS out1 = new TestOS();

      BufferedWalOutputStream bwos = new BufferedWalOutputStream(out1);

      while (position < data.length) {
        if (rand.nextBoolean()) {
          bwos.write(data[position++]);
        } else {
          int len = rand.nextInt(data.length - position) + 1;
          bwos.write(data, position, len);
          position += len;
        }

        if (rand.nextInt(10) == 0) {
          bwos.hflush();
          //System.out.println("hflushed");
        }

        //System.out.println("Wrote "+position+" of "+data.length);

      }

      bwos.close();

      byte[] wrote = out1.toByteArray();

      Assert.assertEquals(0, Arrays.compare(data, wrote));
    }
  }

  @Test
  public void test2() throws IOException {
    Random rand = new Random();

    byte[] data = new byte[10000000];

      int position = 0;

      rand.nextBytes(data);

      TestOS out1 = new TestOS();

      BufferedWalOutputStream bwos = new BufferedWalOutputStream(out1);

      while (position < data.length) {
        bwos.write(data[position++]);
        if (rand.nextInt(100) == 0) {
          bwos.hflush();
          //System.out.println("hflushed");
        }
        //System.out.println("Wrote "+position+" of "+data.length);
      }

      bwos.close();

      byte[] wrote = out1.toByteArray();

      Assert.assertEquals(0, Arrays.compare(data, wrote));
  }
}
