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
package org.apache.accumulo.server.manager.recovery;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.manager.recovery.LogCloser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.viewfs.ViewFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AzureLogCloser implements LogCloser {
  private static final Logger log = LoggerFactory.getLogger(AzureLogCloser.class);

  @Override
  public long close(AccumuloConfiguration conf, Configuration hadoopConf, VolumeManager fs,
      Path source) throws IOException {
    FileSystem ns = fs.getFileSystemByPath(source);

    // if path points to a viewfs path, then resolve to underlying filesystem
    if (ns instanceof ViewFileSystem) {
      Path newSource = ns.resolvePath(source);
      if (!newSource.equals(source) && newSource.toUri().getScheme() != null) {
        ns = newSource.getFileSystem(hadoopConf);
        source = newSource;
      }
    }

    if (ns instanceof AzureBlobFileSystem) {
      AzureBlobFileSystem afs = (AzureBlobFileSystem) ns;
      try {
        afs.breakLease(source);
        log.info("Broke lease on {}", source);
      } catch (Exception ex) {
        try {
          log.warn("Error breaking lease on " + source, ex);
          ns.append(source).close();
          log.info("Obtained lease on {} using append", source);
        } catch (Exception ex2) {
          log.warn("Error obtaining lease on {}, retrying", source, ex);
          return conf.getTimeInMillis(Property.MANAGER_LEASE_RECOVERY_WAITING_PERIOD);
        }
      }
    } else {
      throw new IllegalStateException(AzureLogCloser.class.getName() + " has been configured for " +
          Property.MANAGER_WALOG_CLOSER_IMPLEMETATION + " but walog FS for " + source + " is " +
          ns.getClass().getName());
    }
    return 0;
  }
}
