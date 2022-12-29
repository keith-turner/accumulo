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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TabletCacheImpl extends TabletCache {

  private static final Logger log = LoggerFactory.getLogger(TabletCacheImpl.class);

  // MAX_TEXT represents a TEXT object that is greater than all others. Attempted to use null for
  // this purpose, but there seems to be a bug in TreeMap.tailMap with null. Therefore instead of
  // using null, created MAX_TEXT.
  static final Text MAX_TEXT = new Text();

  static final Comparator<Text> END_ROW_COMPARATOR = (o1, o2) -> {
    if (o1 == o2) {
      return 0;
    }
    if (o1 == MAX_TEXT) {
      return 1;
    }
    if (o2 == MAX_TEXT) {
      return -1;
    }
    return o1.compareTo(o2);
  };

  protected TableId tableId;
  protected TabletCache parent;
  protected TreeMap<Text,CachedTablet> metaCache = new TreeMap<>(END_ROW_COMPARATOR);
  protected TabletLocationObtainer locationObtainer;
  private TabletServerLockChecker lockChecker;
  protected Text lastTabletRow;

  private TreeSet<KeyExtent> badExtents = new TreeSet<>();
  private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Lock rLock = rwLock.readLock();
  private final Lock wLock = rwLock.writeLock();

  public interface TabletLocationObtainer {
    /**
     * @return null when unable to read information successfully
     */
    CachedTablets lookupTablet(ClientContext context, CachedTablet src, Text row, Text stopRow,
        TabletCache parent) throws AccumuloSecurityException, AccumuloException;

    List<CachedTablet> lookupTablets(ClientContext context, String tserver,
        Map<KeyExtent,List<Range>> map, TabletCache parent)
        throws AccumuloSecurityException, AccumuloException;

    Mode getMode();
  }

  public interface TabletServerLockChecker {
    boolean isLockHeld(String tserver, String session);

    void invalidateCache(String server);
  }

  private class LockCheckerSession {

    private HashSet<Pair<String,String>> okLocks = new HashSet<>();
    private HashSet<Pair<String,String>> invalidLocks = new HashSet<>();

    private CachedTablet checkLock(CachedTablet tl) {
      // the goal of this class is to minimize calls out to lockChecker under that assumption that
      // its a resource synchronized among many threads... want to
      // avoid fine grained synchronization when binning lots of mutations or ranges... remember
      // decisions from the lockChecker in thread local unsynchronized
      // memory

      if (tl == null) {
        return null;
      }

      if (!tl.hasTserverLocation()) {
        return tl;
      }

      Pair<String,String> lock = new Pair<>(tl.getTserverLocation(), tl.getTserverSession());

      if (okLocks.contains(lock)) {
        return tl;
      }

      if (invalidLocks.contains(lock)) {
        return null;
      }

      if (lockChecker.isLockHeld(tl.getTserverLocation(), tl.getTserverSession())) {
        okLocks.add(lock);
        return tl;
      }

      if (log.isTraceEnabled()) {
        log.trace("Tablet server {} {} no longer holds its lock", tl.getTserverLocation(),
            tl.getTserverSession());
      }

      invalidLocks.add(lock);

      return null;
    }
  }

  public TabletCacheImpl(TableId tableId, TabletCache parent, TabletLocationObtainer tlo,
      TabletServerLockChecker tslc) {
    this.tableId = tableId;
    this.parent = parent;
    this.locationObtainer = tlo;
    this.lockChecker = tslc;

    this.lastTabletRow = new Text(tableId.canonical());
    lastTabletRow.append(new byte[] {'<'}, 0, 1);
  }

  @Override
  public <T extends Mutation> void binMutations(ClientContext context, List<T> mutations,
      Map<String,TabletServerMutations<T>> binnedMutations, List<T> failures)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Binning {} mutations for table {}", Thread.currentThread().getId(),
          mutations.size(), tableId);
      timer = new OpTimer().start();
    }

    ArrayList<T> notInCache = new ArrayList<>();
    Text row = new Text();

    LockCheckerSession lcSession = new LockCheckerSession();

    rLock.lock();
    try {
      processInvalidated(context, lcSession);

      // for this to be efficient rows need to be in sorted order, but always sorting is slow...
      // therefore only sort the
      // stuff not in the cache.... it is most efficient to pass _locateTablet rows in sorted order

      // For this to be efficient, need to avoid fine grained synchronization and fine grained
      // logging.
      // Therefore methods called by this are not synchronized and should not log.

      for (T mutation : mutations) {
        row.set(mutation.getRow());
        CachedTablet tl = locateTabletInCache(row);
        if (tl == null || !addMutation(binnedMutations, mutation, tl, lcSession)) {
          notInCache.add(mutation);
        }
      }
    } finally {
      rLock.unlock();
    }

    if (!notInCache.isEmpty()) {
      notInCache.sort((o1, o2) -> WritableComparator.compareBytes(o1.getRow(), 0,
          o1.getRow().length, o2.getRow(), 0, o2.getRow().length));

      wLock.lock();
      try {
        boolean failed = false;
        for (T mutation : notInCache) {
          if (failed) {
            // when one table does not return a location, something is probably
            // screwy, go ahead and fail everything.
            failures.add(mutation);
            continue;
          }

          row.set(mutation.getRow());

          CachedTablet tl = _locateTablet(context, row, false, false, false, lcSession);

          if (tl == null || !addMutation(binnedMutations, mutation, tl, lcSession)) {
            failures.add(mutation);
            failed = true;
          }
        }
      } finally {
        wLock.unlock();
      }
    }

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Binned {} mutations for table {} to {} tservers in {}",
          Thread.currentThread().getId(), mutations.size(), tableId, binnedMutations.size(),
          String.format("%.3f secs", timer.scale(SECONDS)));
    }

  }

  private <T extends Mutation> boolean addMutation(
      Map<String,TabletServerMutations<T>> binnedMutations, T mutation, CachedTablet tl,
      LockCheckerSession lcSession) {
    TabletServerMutations<T> tsm = binnedMutations.get(tl.getTserverLocation());

    if (tsm == null) {
      // do lock check once per tserver here to make binning faster
      boolean lockHeld = lcSession.checkLock(tl) != null;
      if (lockHeld) {
        tsm = new TabletServerMutations<>(tl.getTserverSession());
        binnedMutations.put(tl.getTserverLocation(), tsm);
      } else {
        return false;
      }
    }

    // its possible the same tserver could be listed with different sessions
    if (tsm.getSession().equals(tl.getTserverSession())) {
      tsm.addMutation(tl.getExtent(), mutation);
      return true;
    }

    return false;
  }

  private List<Range> locateTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer, boolean useCache, LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    List<Range> failures = new ArrayList<>();
    List<CachedTablet> cachedTablets = new ArrayList<>();

    boolean lookupFailed = false;

    l1: for (Range range : ranges) {

      cachedTablets.clear();

      Text startRow;

      if (range.getStartKey() != null) {
        startRow = range.getStartKey().getRow();
      } else {
        startRow = new Text();
      }

      CachedTablet tl = null;

      if (useCache) {
        tl = lcSession.checkLock(locateTabletInCache(startRow));
      } else if (!lookupFailed) {
        tl = _locateTablet(context, startRow, false, false, false, lcSession);
      }

      if (tl == null) {
        failures.add(range);
        if (!useCache) {
          lookupFailed = true;
        }
        continue;
      }

      cachedTablets.add(tl);

      while (tl.getExtent().endRow() != null
          && !range.afterEndKey(new Key(tl.getExtent().endRow()).followingKey(PartialKey.ROW))) {
        if (useCache) {
          Text row = new Text(tl.getExtent().endRow());
          row.append(new byte[] {0}, 0, 1);
          tl = lcSession.checkLock(locateTabletInCache(row));
        } else {
          tl = _locateTablet(context, tl.getExtent().endRow(), true, false, false, lcSession);
        }

        if (tl == null) {
          failures.add(range);
          if (!useCache) {
            lookupFailed = true;
          }
          continue l1;
        }
        cachedTablets.add(tl);
      }

      for (CachedTablet tl2 : cachedTablets) {
        rangeConsumer.accept(tl2, range);
      }

    }

    return failures;
  }

  @Override
  public List<Range> locateTablets(ClientContext context, List<Range> ranges,
      BiConsumer<CachedTablet,Range> rangeConsumer)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    /*
     * For this to be efficient, need to avoid fine grained synchronization and fine grained
     * logging. Therefore methods called by this are not synchronized and should not log.
     */

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Binning {} ranges for table {}", Thread.currentThread().getId(),
          ranges.size(), tableId);
      timer = new OpTimer().start();
    }

    LockCheckerSession lcSession = new LockCheckerSession();

    List<Range> failures;
    rLock.lock();
    try {
      processInvalidated(context, lcSession);

      // for this to be optimal, need to look ranges up in sorted order when
      // ranges are not present in cache... however do not want to always
      // sort ranges... therefore try binning ranges using only the cache
      // and sort whatever fails and retry

      failures = locateTablets(context, ranges, rangeConsumer, true, lcSession);
    } finally {
      rLock.unlock();
    }

    if (!failures.isEmpty()) {
      // sort failures by range start key
      Collections.sort(failures);

      // try lookups again
      wLock.lock();
      try {
        failures = locateTablets(context, failures, rangeConsumer, false, lcSession);
      } finally {
        wLock.unlock();
      }
    }

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Binned {} ranges for table {} in {}", Thread.currentThread().getId(),
          ranges.size(), tableId, String.format("%.3f secs", timer.scale(SECONDS)));
    }

    return failures;
  }

  @Override
  public void invalidateCache(KeyExtent failedExtent) {
    wLock.lock();
    try {
      badExtents.add(failedExtent);
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled()) {
      log.trace("Invalidated extent={}", failedExtent);
    }
  }

  @Override
  public void invalidateCache(Collection<KeyExtent> keySet) {
    wLock.lock();
    try {
      badExtents.addAll(keySet);
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled()) {
      log.trace("Invalidated {} cache entries for table {}", keySet.size(), tableId);
    }
  }

  @Override
  public void invalidateCache(ClientContext context, String server) {
    int invalidatedCount = 0;

    wLock.lock();
    try {
      for (CachedTablet cacheEntry : metaCache.values()) {
        if (cacheEntry.hasTserverLocation() && cacheEntry.getTserverLocation().equals(server)) {
          badExtents.add(cacheEntry.getExtent());
          invalidatedCount++;
        }
      }
    } finally {
      wLock.unlock();
    }

    lockChecker.invalidateCache(server);

    if (log.isTraceEnabled()) {
      log.trace("invalidated {} cache entries  table={} server={}", invalidatedCount, tableId,
          server);
    }

  }

  @Override
  public Mode getMode() {
    return locationObtainer.getMode();
  }

  @Override
  public void invalidateCache() {
    int invalidatedCount;
    wLock.lock();
    try {
      invalidatedCount = metaCache.size();
      metaCache.clear();
    } finally {
      wLock.unlock();
    }
    if (log.isTraceEnabled()) {
      log.trace("invalidated all {} cache entries for table={}", invalidatedCount, tableId);
    }
  }

  @Override
  public CachedTablet locateTablet(ClientContext context, Text row, boolean skipRow, boolean retry)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Locating tablet  table={} row={} skipRow={} retry={}",
          Thread.currentThread().getId(), tableId, TextUtil.truncate(row), skipRow, retry);
      timer = new OpTimer().start();
    }

    while (true) {

      LockCheckerSession lcSession = new LockCheckerSession();
      CachedTablet tl = _locateTablet(context, row, skipRow, retry, true, lcSession);

      if (retry && tl == null) {
        sleepUninterruptibly(100, MILLISECONDS);
        if (log.isTraceEnabled()) {
          log.trace("Failed to locate tablet containing row {} in table {}, will retry...",
              TextUtil.truncate(row), tableId);
        }
        continue;
      }

      if (timer != null) {
        timer.stop();
        log.trace("tid={} Located tablet {} at {} in {}", Thread.currentThread().getId(),
            (tl == null ? "null" : tl.getExtent()),
            (tl == null || !tl.hasTserverLocation() ? "null" : tl.getTserverLocation()),
            String.format("%.3f secs", timer.scale(SECONDS)));
      }

      return tl;
    }
  }

  private void lookupTabletLocation(ClientContext context, Text row, boolean retry,
      LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Text metadataRow = new Text(tableId.canonical());
    metadataRow.append(new byte[] {';'}, 0, 1);
    metadataRow.append(row.getBytes(), 0, row.getLength());
    CachedTablet ptl = parent.locateTablet(context, metadataRow, false, retry);

    if (ptl != null) {
      CachedTablets locations =
          locationObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow, parent);
      while (locations != null && locations.getLocations().isEmpty()
          && locations.getLocationless().isEmpty()) {
        // try the next tablet, the current tablet does not have any tablets that overlap the row
        Text er = ptl.getExtent().endRow();
        if (er != null && er.compareTo(lastTabletRow) < 0) {
          // System.out.println("er "+er+" ltr "+lastTabletRow);
          ptl = parent.locateTablet(context, er, true, retry);
          if (ptl != null) {
            locations =
                locationObtainer.lookupTablet(context, ptl, metadataRow, lastTabletRow, parent);
          } else {
            break;
          }
        } else {
          break;
        }
      }

      if (locations == null) {
        return;
      }

      // cannot assume the list contains contiguous key extents... so it is probably
      // best to deal with each extent individually

      Text lastEndRow = null;
      for (CachedTablet cachedTablet : locations.getLocations()) {

        KeyExtent ke = cachedTablet.getExtent();
        CachedTablet locToCache;

        // create new location if current prevEndRow == endRow
        if ((lastEndRow != null) && (ke.prevEndRow() != null)
            && ke.prevEndRow().equals(lastEndRow)) {
          locToCache =
              new CachedTablet(new KeyExtent(ke.tableId(), ke.endRow(), lastEndRow), cachedTablet);
        } else {
          locToCache = cachedTablet;
        }

        // save endRow for next iteration
        lastEndRow = locToCache.getExtent().endRow();

        updateCache(locToCache, lcSession);
      }
    }

  }

  private void updateCache(CachedTablet cachedTablet, LockCheckerSession lcSession) {
    if (!cachedTablet.getExtent().tableId().equals(tableId)) {
      // sanity check
      throw new IllegalStateException(
          "Unexpected extent returned " + tableId + "  " + cachedTablet.getExtent());
    }

    if (getMode() == TabletCache.Mode.ONLINE && !cachedTablet.hasTserverLocation()) {
      // sanity check
      throw new IllegalStateException(
          "Cannot add null locations to cache " + tableId + "  " + cachedTablet.getExtent());
    }

    // clear out any overlapping extents in cache
    removeOverlapping(metaCache, cachedTablet.getExtent());

    // do not add to cache unless lock is held
    if (lcSession.checkLock(cachedTablet) == null) {
      return;
    }

    // add it to cache
    Text er = cachedTablet.getExtent().endRow();
    if (er == null) {
      er = MAX_TEXT;
    }
    metaCache.put(er, cachedTablet);

    if (!badExtents.isEmpty()) {
      removeOverlapping(badExtents, cachedTablet.getExtent());
    }
  }

  static void removeOverlapping(TreeMap<Text,CachedTablet> metaCache, KeyExtent nke) {
    Iterator<Entry<Text,CachedTablet>> iter = null;

    if (nke.prevEndRow() == null) {
      iter = metaCache.entrySet().iterator();
    } else {
      Text row = rowAfterPrevRow(nke);
      SortedMap<Text,CachedTablet> tailMap = metaCache.tailMap(row);
      iter = tailMap.entrySet().iterator();
    }

    while (iter.hasNext()) {
      Entry<Text,CachedTablet> entry = iter.next();

      KeyExtent ke = entry.getValue().getExtent();

      if (stopRemoving(nke, ke)) {
        break;
      }

      iter.remove();
    }
  }

  private static boolean stopRemoving(KeyExtent nke, KeyExtent ke) {
    return ke.prevEndRow() != null && nke.endRow() != null
        && ke.prevEndRow().compareTo(nke.endRow()) >= 0;
  }

  private static Text rowAfterPrevRow(KeyExtent nke) {
    Text row = new Text(nke.prevEndRow());
    row.append(new byte[] {0}, 0, 1);
    return row;
  }

  static void removeOverlapping(TreeSet<KeyExtent> extents, KeyExtent nke) {
    for (KeyExtent overlapping : KeyExtent.findOverlapping(nke, extents)) {
      extents.remove(overlapping);
    }
  }

  private CachedTablet locateTabletInCache(Text row) {

    Entry<Text,CachedTablet> entry = metaCache.ceilingEntry(row);

    if (entry != null) {
      KeyExtent ke = entry.getValue().getExtent();
      if (ke.prevEndRow() == null || ke.prevEndRow().compareTo(row) < 0) {
        return entry.getValue();
      }
    }
    return null;
  }

  protected CachedTablet _locateTablet(ClientContext context, Text row, boolean skipRow,
      boolean retry, boolean lock, LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    if (skipRow) {
      row = new Text(row);
      row.append(new byte[] {0}, 0, 1);
    }

    CachedTablet tl;

    if (lock) {
      rLock.lock();
      try {
        tl = processInvalidatedAndCheckLock(context, lcSession, row);
      } finally {
        rLock.unlock();
      }
    } else {
      tl = processInvalidatedAndCheckLock(context, lcSession, row);
    }

    if (tl == null) {
      // not in cache, so obtain info
      if (lock) {
        wLock.lock();
        try {
          tl = lookupTabletLocationAndCheckLock(context, row, retry, lcSession);
        } finally {
          wLock.unlock();
        }
      } else {
        tl = lookupTabletLocationAndCheckLock(context, row, retry, lcSession);
      }
    }

    return tl;
  }

  private CachedTablet lookupTabletLocationAndCheckLock(ClientContext context, Text row,
      boolean retry, LockCheckerSession lcSession)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    lookupTabletLocation(context, row, retry, lcSession);
    return lcSession.checkLock(locateTabletInCache(row));
  }

  private CachedTablet processInvalidatedAndCheckLock(ClientContext context,
      LockCheckerSession lcSession, Text row)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    processInvalidated(context, lcSession);
    return lcSession.checkLock(locateTabletInCache(row));
  }

  @SuppressFBWarnings(value = {"UL_UNRELEASED_LOCK", "UL_UNRELEASED_LOCK_EXCEPTION_PATH"},
      justification = "locking is confusing, but probably correct")
  private void processInvalidated(ClientContext context, LockCheckerSession lcSession)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    if (badExtents.isEmpty()) {
      return;
    }

    final boolean writeLockHeld = rwLock.isWriteLockedByCurrentThread();
    try {
      if (!writeLockHeld) {
        rLock.unlock();
        wLock.lock();
        if (badExtents.isEmpty()) {
          return;
        }
      }

      List<Range> lookups = new ArrayList<>(badExtents.size());

      for (KeyExtent be : badExtents) {
        lookups.add(be.toMetaRange());
        removeOverlapping(metaCache, be);
      }

      lookups = Range.mergeOverlapping(lookups);

      Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

      parent.locateTablets(context, lookups,
          (cachedTablet, range) -> addRange(binnedRanges, cachedTablet, range));

      // randomize server order
      ArrayList<String> tabletServers = new ArrayList<>(binnedRanges.keySet());
      Collections.shuffle(tabletServers);

      for (String tserver : tabletServers) {
        List<CachedTablet> locations =
            locationObtainer.lookupTablets(context, tserver, binnedRanges.get(tserver), parent);

        for (CachedTablet cachedTablet : locations) {
          updateCache(cachedTablet, lcSession);
        }
      }
    } finally {
      if (!writeLockHeld) {
        rLock.lock();
        wLock.unlock();
      }
    }
  }

  static void addRange(Map<String,Map<KeyExtent,List<Range>>> binnedRanges, CachedTablet ct,
      Range range) {
    binnedRanges.computeIfAbsent(ct.getTserverLocation(), k -> new HashMap<>())
        .computeIfAbsent(ct.getExtent(), k -> new ArrayList<>()).add(range);
  }
}
