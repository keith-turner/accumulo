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
package org.apache.accumulo.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO WrappingIter can be inefficient (extra indirection), implement the interface directly since overriding all methods
public class StatsIterator extends WrappingIterator implements OptionDescriber {

    private String prefix;

    private static final Logger log = LoggerFactory.getLogger(DebugIterator.class);

    public StatsIterator() {}

    public static class IteratorStats {
        long seeks;
        long nextCalls;

        long keyBytes;
        long getTopKeyCalls;
        long valueBytes;
        long getTopValueCalls;

        long seekNanos;
        long nextNanos;
    }

    private IteratorStats stats = new IteratorStats();

    public IteratorStats getStats(){
        return stats;
    }

    @Override
    public StatsIterator deepCopy(IteratorEnvironment env) {
        return new StatsIterator(this, env);
    }

    private StatsIterator(StatsIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
        prefix = other.prefix;
    }

    public StatsIterator(SortedKeyValueIterator<Key,Value> source) {
        this.setSource(source);
    }

    @Override
    public Key getTopKey() {
        Key wc = super.getTopKey();
        stats.keyBytes+=wc.getLength();
        stats.getTopKeyCalls++;
        return wc;
    }

    @Override
    public Value getTopValue() {
        Value w = super.getTopValue();
        stats.valueBytes+=w.getSize();
        stats.getTopValueCalls++;
        return w;
    }

    @Override
    public boolean hasTop() {
        boolean b = super.hasTop();
        return b;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
            throws IOException {
        long t1 = System.currentTimeMillis();
        super.seek(range, columnFamilies, inclusive);
        long t2=System.currentTimeMillis();
        stats.seeks++;
        stats.seekNanos += (t2-t1);
    }

    @Override
    public void next() throws IOException {
        long t1 = System.nanoTime();
        super.next();
        long t2 = System.nanoTime();
        stats.nextCalls++;
        stats.nextNanos += (t2-t1);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                     IteratorEnvironment env) throws IOException {
        if (prefix == null) {
            prefix = String.format("0x%08X", this.hashCode());
        }

        super.init(source, options, env);
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("debug",
                DebugIterator.class.getSimpleName()
                        + " prints debug information on each SortedKeyValueIterator method invocation",
                null, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
}