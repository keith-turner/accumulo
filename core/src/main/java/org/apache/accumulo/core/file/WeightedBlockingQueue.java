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
package org.apache.accumulo.core.file;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

public class WeightedBlockingQueue<E> {

  private final ArrayDeque<Element<E>> queue;
  private final Weigher<E> weigher;
  private final long maxWeight;
  private long queuedWeight = 0;

  private final Lock lock;
  private final Condition putCondition;
  private final Condition takeCondition;

  private static class Element<E> {
    final E e;
    final int weight;

    private Element(E e, int weight) {
      this.e = e;
      this.weight = weight;
    }
  }

  public interface Weigher<E> {
    int weigh(E e);
  }

  public WeightedBlockingQueue(Weigher<E> weigher, long maxWeight) {
    this.weigher = weigher;
    this.queue = new ArrayDeque<>();
    this.maxWeight = maxWeight;
    this.lock = new ReentrantLock();
    this.putCondition = lock.newCondition();
    this.takeCondition = lock.newCondition();
  }

  public void put(E e) throws InterruptedException {
    lock.lock();
    try {

      Objects.requireNonNull(e);
      int weight = weigher.weigh(e);
      Preconditions.checkState(weight > 0);

      if (weight > maxWeight) {
        // when a single element exceeds the max weight, then wait for the queue to be empty
        while (!queue.isEmpty()) {
          takeCondition.await();
        }
      } else {
        while (queuedWeight + weight > maxWeight) {
          takeCondition.await();
        }
      }

      queuedWeight += weight;
      queue.addFirst(new Element<>(e, weight));
      putCondition.signal();
    } finally {
      lock.unlock();
    }
  }

  public E take() throws InterruptedException {
    lock.lock();
    try {
      Element<E> element;
      while ((element = queue.pollLast()) == null) {
        putCondition.await();
      }

      queuedWeight -= element.weight;
      Preconditions.checkState(queuedWeight >= 0);

      takeCondition.signal();

      return element.e;
    } finally {
      lock.unlock();
    }
  }

  public int drainTo(Collection<? super E> c) {
    lock.lock();
    try {
      Element<E> element = queue.pollLast();
      int added = 0;
      while (element != null) {
        queuedWeight -= element.weight;
        Preconditions.checkState(queuedWeight >= 0);
        c.add(element.e);
        element = queue.pollLast();
        added++;
      }

      if (added > 0) {
        takeCondition.signal();
      }
      return added;
    } finally {
      lock.unlock();
    }
  }

  public boolean isEmpty() {
    lock.lock();
    try {
      return queue.isEmpty();
    } finally {
      lock.unlock();
    }
  }
}
