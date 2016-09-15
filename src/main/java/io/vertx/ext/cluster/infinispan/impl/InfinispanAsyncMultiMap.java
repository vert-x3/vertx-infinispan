/*
 * Copyright 2016 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.ext.cluster.infinispan.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.stream.CacheCollectors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.infinispan.notifications.Listener.Observation.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

  private final Vertx vertx;
  private final Cache<MultiMapKey, Object> cache;

  private final ConcurrentMap<K, ChoosableSet<V>> nearCache;
  private final AtomicInteger getInProgressCount;

  public InfinispanAsyncMultiMap(Vertx vertx, Cache<MultiMapKey, Object> cache) {
    this.vertx = vertx;
    this.cache = cache;
    nearCache = new ConcurrentHashMap<>();
    getInProgressCount = new AtomicInteger();
    cache.addListener(new EntryListener());
    cache.getCacheManager().addListener(new ViewChangeListener());
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Context context = vertx.getOrCreateContext();
    FutureAdapter<Object> futureAdapter = new FutureAdapter<>(context);
    cache.putAsync(new MultiMapKey(kk, vv), MeaningLessValue.INSTANCE).attachListener(futureAdapter);
    futureAdapter.getVertxFuture().map((Void) null).setHandler(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
    ChoosableSet<V> entries = nearCache.get(k);
    if (entries != null && entries.isInitialised() && getInProgressCount.get() == 0) {
      resultHandler.handle(Future.succeededFuture(entries));
    } else {
      getInProgressCount.incrementAndGet();
      vertx.<ChoosableIterable<V>>executeBlocking(fut -> {
        List<MultiMapKey> collect = cache.keySet().parallelStream()
          .filter(new KeyEqualsPredicate(DataConverter.toCachedObject(k)))
          .collect(CacheCollectors.serializableCollector(Collectors::toList));
        Collection<V> entries2 = collect.stream()
          .map(mmk -> DataConverter.<V>fromCachedObject(mmk.getValue()))
          .collect(toList());
        ChoosableSet<V> sids;
        if (entries2 != null) {
          sids = new ChoosableSet<>(entries2.size());
          for (V hid : entries2) {
            sids.add(hid);
          }
        } else {
          sids = new ChoosableSet<>(0);
        }
        ChoosableSet<V> prev = (sids.isEmpty()) ? null : nearCache.putIfAbsent(k, sids);
        if (prev != null) {
          // Merge them
          prev.merge(sids);
          sids = prev;
        }
        sids.setInitialised();
        fut.complete(sids);
      }, res -> {
        getInProgressCount.decrementAndGet();
        resultHandler.handle(res);
      });
    }
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Context context = vertx.getOrCreateContext();
    FutureAdapter<Boolean> futureAdapter = new FutureAdapter<>(context);
    cache.removeAsync(new MultiMapKey(kk, vv), MeaningLessValue.INSTANCE).attachListener(futureAdapter);
    futureAdapter.getVertxFuture().setHandler(completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    Object vv = DataConverter.toCachedObject(v);
    vertx.executeBlocking(future -> {
      for (Iterator<MultiMapKey> iterator = cache.keySet().iterator(); iterator.hasNext(); ) {
        MultiMapKey next = iterator.next();
        if (next.getValue().equals(vv)) {
          iterator.remove();
        }
      }
      future.complete();
    }, false, completionHandler);
  }

  @Listener(clustered = true, observation = POST, sync = false)
  private class EntryListener {
    @CacheEntryCreated
    public void entryCreated(CacheEntryCreatedEvent<MultiMapKey, Object> event) {
      MultiMapKey multiMapKey = event.getKey();
      K k = DataConverter.fromCachedObject(multiMapKey.getKey());
      V v = DataConverter.fromCachedObject(multiMapKey.getValue());
      ChoosableSet<V> entries = nearCache.get(k);
      if (entries == null) {
        entries = new ChoosableSet<>(1);
        ChoosableSet<V> prev = nearCache.putIfAbsent(k, entries);
        if (prev != null) {
          entries = prev;
        }
      }
      entries.add(v);
    }

    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent<MultiMapKey, Object> event) {
      MultiMapKey multiMapKey = event.getKey();
      K k = DataConverter.fromCachedObject(multiMapKey.getKey());
      V v = DataConverter.fromCachedObject(multiMapKey.getValue());
      ChoosableSet<V> entries = nearCache.get(k);
      if (entries != null) {
        entries.remove(v);
        if (entries.isEmpty()) {
          nearCache.remove(k);
        }
      }
    }
  }

  @SerializeWith(MultiMapKey.MultiMapKeyExternalizer.class)
  public static class MultiMapKey {
    private final Object key;
    private final Object value;


    public MultiMapKey(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    public Object getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MultiMapKey that = (MultiMapKey) o;
      return key.equals(that.key) && value.equals(that.value);

    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + value.hashCode();
      return result;
    }

    public static class MultiMapKeyExternalizer implements Externalizer<MultiMapKey> {
      @Override
      public void writeObject(ObjectOutput output, MultiMapKey object) throws IOException {
        output.writeObject(object.key);
        output.writeObject(object.value);
      }

      @Override
      @SuppressWarnings("unchecked")
      public MultiMapKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        return new MultiMapKey(input.readObject(), input.readObject());
      }
    }
  }

  @SerializeWith(MeaningLessValue.MeaningLessValueExternalizer.class)
  public static class MeaningLessValue {
    public static final MeaningLessValue INSTANCE = new MeaningLessValue();

    private MeaningLessValue() {
    }

    public static class MeaningLessValueExternalizer implements Externalizer<MeaningLessValue> {
      @Override
      public void writeObject(ObjectOutput output, MeaningLessValue object) throws IOException {
      }

      @Override
      public MeaningLessValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        return MeaningLessValue.INSTANCE;
      }
    }
  }

  @SerializeWith(KeyEqualsPredicate.KeyEqualsPredicateExternalizer.class)
  public static class KeyEqualsPredicate implements Predicate<MultiMapKey> {

    private final Object kk;

    public KeyEqualsPredicate(Object kk) {
      this.kk = kk;
    }

    @Override
    public boolean test(MultiMapKey mmk) {
      return mmk.getKey().equals(kk);
    }

    public static class KeyEqualsPredicateExternalizer implements Externalizer<KeyEqualsPredicate> {

      @Override
      public void writeObject(ObjectOutput output, KeyEqualsPredicate object) throws IOException {
        output.writeObject(object.kk);
      }

      @Override
      public KeyEqualsPredicate readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        return new KeyEqualsPredicate(input.readObject());
      }
    }
  }

  /**
   * @author <a href="http://tfox.org">Tim Fox</a>
   */
  private static class ChoosableSet<T> implements ChoosableIterable<T> {

    private volatile boolean initialised;
    private final Set<T> ids;
    private volatile Iterator<T> iter;

    public ChoosableSet(int initialSize) {
      ids = new ConcurrentHashSet<>(initialSize);
    }

    public boolean isInitialised() {
      return initialised;
    }

    public void setInitialised() {
      this.initialised = true;
    }

    public void add(T elem) {
      ids.add(elem);
    }

    public void remove(T elem) {
      ids.remove(elem);
    }

    public void merge(ChoosableSet<T> toMerge) {
      ids.addAll(toMerge.ids);
    }

    public boolean isEmpty() {
      return ids.isEmpty();
    }

    @Override
    public Iterator<T> iterator() {
      return ids.iterator();
    }

    public synchronized T choose() {
      if (!ids.isEmpty()) {
        if (iter == null || !iter.hasNext()) {
          iter = ids.iterator();
        }
        try {
          return iter.next();
        } catch (NoSuchElementException e) {
          return null;
        }
      } else {
        return null;
      }
    }
  }

  @Listener(sync = false)
  private class ViewChangeListener {
    @ViewChanged
    public void handleViewChange(final ViewChangedEvent e) {
      if (e.isMergeView()) {
        // In case we're merging two partitions, make sure all nodes see the same data
        InfinispanAsyncMultiMap.this.nearCache.clear();
      }
    }
  }
}
