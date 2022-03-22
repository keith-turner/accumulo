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
package org.apache.accumulo.core.metadata;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Objects;
import java.util.UUID;

import org.apache.accumulo.core.clientImpl.lexicoder.ByteUtils;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class ScanServerRefTabletFile extends TabletFile {

  private final Value NULL_VALUE = new Value(new byte[0]);
  private final Text colf;
  private final Text colq;

  public static Text createColf(String serverAddress, UUID serverLockUUID) {
    Objects.requireNonNull(serverAddress, "address must be supplied");
    Objects.requireNonNull(serverLockUUID, "uuid must be supplied");
    return new Text(
        ByteUtils.concat(serverAddress.getBytes(UTF_8), serverLockUUID.toString().getBytes(UTF_8)));
  }

  public static Pair<String,UUID> parseColf(Text colf) {
    byte[][] parts = ByteUtils.split(colf.getBytes());
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          "ScanServerRefTabletFile colf in wrong format: " + colf.toString());
    }
    return new Pair<>(new String(parts[0], UTF_8), UUID.fromString(new String(parts[1], UTF_8)));
  }

  public ScanServerRefTabletFile(String file, String serverAddress, UUID serverLockUUID,
      String clientAddress) {
    super(new Path(file));
    this.colf = createColf(serverAddress, serverLockUUID);
    this.colq = new Text(clientAddress.getBytes(UTF_8));
  }

  public ScanServerRefTabletFile(String file, Text colf, Text colq) {
    super(new Path(file));
    this.colf = colf;
    this.colq = colq;
  }

  public String getRowSuffix() {
    return this.getPathStr();
  }

  public Text getServerAddress() {
    return this.colf;
  }

  public Text getClientAddress() {
    return this.colq;
  }

  public Value getValue() {
    return NULL_VALUE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((colf == null) ? 0 : colf.hashCode());
    result = prime * result + ((colq == null) ? 0 : colq.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    ScanServerRefTabletFile other = (ScanServerRefTabletFile) obj;
    if (colf == null) {
      if (other.colf != null)
        return false;
    } else if (!colf.equals(other.colf))
      return false;
    if (colq == null) {
      if (other.colq != null)
        return false;
    } else if (!colq.equals(other.colq))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ScanServerRefTabletFile [file=" + this.getRowSuffix() + ", server address="
        + parseColf(colf) + ", client address=" + colq + "]";
  }

}
