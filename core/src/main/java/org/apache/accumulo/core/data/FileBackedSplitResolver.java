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
package org.apache.accumulo.core.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorAdapter;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.hadoop.io.Text;

public class FileBackedSplitResolver {
  /**
   *
   */
  public static void createFile(String outputFile, SortedMap<TableId,Iterable<Text>> tableSplits)
      throws IOException {
    Map<String,String> props = Map.of(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "4K",
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX.getKey(), "100K");
    try (var writer = RFile.newWriter().to(outputFile).withTableProperties(props).build()) {
      for (var tableEntry : tableSplits.entrySet()) {
        var tableId = tableEntry.getKey();
        var splits = tableEntry.getValue();
        Text prev = null;
        for (Text endRow : splits) {
          var extent = new KeyExtent(tableId, endRow, prev);
          var row = extent.toMetaRow();
          var key = new Key(row);
          key.setTimestamp(0);
          var encodedPrev = TabletColumnFamily.encodePrevEndRow(extent.prevEndRow());
          writer.append(key, encodedPrev);
          prev = endRow;
        }
        var extent = new KeyExtent(tableId, null, prev);
        var row = extent.toMetaRow();
        var key = new Key(row);
        key.setTimestamp(0);
        var encodedPrev = TabletColumnFamily.encodePrevEndRow(extent.prevEndRow());
        writer.append(key, encodedPrev);
      }
    }
  }

  public static LoadPlan.SplitResolver createResolver(String file, TableId table, long cacheSize) {
    long dataCacheSize = (long) (.9 * cacheSize);
    long indexCacheSize = (long) (.1 * cacheSize);
    var scanner = RFile.newScanner().from(file).withoutSystemIterators()
        .withDataCache(dataCacheSize).withIndexCache(indexCacheSize).build();

    boolean useHack = true;
    if (useHack) {
      // This is a hack that massively speeds up the lookup by resuing the accumulo iterator,
      // instead of completely recreating it each time RFileScanner.iterator() is called. There is
      // no way to get at this via public API.
      IteratorAdapter iteratorAdapter = (IteratorAdapter) scanner.iterator();
      var aiter = iteratorAdapter.getAccumuloIter();

      return row -> {
        var lookupRow = MetadataSchema.TabletsSection.encodeRow(table, row);
        try {
          aiter.seek(new Range(lookupRow, null), Set.of(), false);
          if (aiter.hasTop()) {
            var endRow = MetadataSchema.TabletsSection.decodeRow(aiter.getTopKey().getRow());
            var prevRow = TabletColumnFamily.decodePrevEndRow(aiter.getTopValue());
            return new LoadPlan.TableSplits(prevRow, endRow.getSecond());
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // TODO this means data is not present in file for this table
        throw new IllegalStateException();
      };
    } else {
      return row -> {
        var lookupRow = MetadataSchema.TabletsSection.encodeRow(table, row);
        // TODO can multiple threads call this?

        scanner.setRange(new Range(lookupRow, null));
        var iter = scanner.iterator();
        if (iter.hasNext()) {
          Map.Entry<Key,Value> entry = iter.next();
          var endRow = MetadataSchema.TabletsSection.decodeRow(entry.getKey().getRow());
          var prevRow = TabletColumnFamily.decodePrevEndRow(entry.getValue());
          return new LoadPlan.TableSplits(prevRow, endRow.getSecond());
        }

        // TODO this means data is not present in file for this table
        throw new IllegalStateException();
      };
    }
  }

  public static void main(String[] args) throws Exception {
    var splitsFile = System.getenv("HOME") + "/all-splits.txt";
    List<Text> splits = Files.lines(Path.of(splitsFile)).map(l -> l.split("\t")[1])
        .map(s -> new Text(Base64.getDecoder().decode(s))).sorted().collect(Collectors.toList());

    SortedMap<TableId,Iterable<Text>> tableSplits = new TreeMap<>(Map.of(TableId.of("3"), splits));
    long t1 = System.currentTimeMillis();
    createFile("/tmp/tableSplits.rf", tableSplits);
    long t2 = System.currentTimeMillis();
    System.out.printf("Wrote %,d splits in %dms\n", splits.size(), t2 - t1);

    // PrintInfo.main(new String[]{"/tmp/tableSplits.rf"});

    doLookups("last 1 million", splits.subList(splits.size() - 1000000, splits.size()));

    ArrayList<Text> copy = new ArrayList<>();
    for (int i = 0; i < splits.size(); i += 100) {
      copy.add(splits.get(i));
    }
    doLookups("every 100th", copy);
  }

  private static void doLookups(String name, Iterable<Text> splits) throws Exception {
    var splitResolver = createResolver("/tmp/tableSplits.rf", TableId.of("3"), 100_000_000);
    doLookups(name, splits, splitResolver);
  }

  private static void doLookups(String name, Iterable<Text> splits,
      LoadPlan.SplitResolver splitResolver) {
    int lookups = 0;
    long t1 = System.currentTimeMillis();
    for (var split : splits) {
      var found = splitResolver.apply(split);
      if (!Objects.equals(found.getEndRow(), split)) {
        throw new IllegalStateException(found.getEndRow() + " != " + split);
      }
      lookups++;
    }
    long t2 = System.currentTimeMillis();

    System.out.printf("%15s : Looked up %,d splits in %,dms rate %6.2f lookups/sec\n", name,
        lookups, t2 - t1, lookups / ((t2 - t1) / 1000.0));
  }
}
