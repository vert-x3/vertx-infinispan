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
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.ContextImpl;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
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
import org.infinispan.stream.CacheCollectors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.infinispan.notifications.Listener.Observation.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

  private final VertxInternal vertx;
  private final Cache<MultiMapKey, Object> cache;
  private final EntryListener listener;
  private final ConcurrentMap<K, ChoosableSet<V>> nearCache;
  private final TaskQueue taskQueue;

  public InfinispanAsyncMultiMap(Vertx vertx, Cache<MultiMapKey, Object> cache) {
    this.vertx = (VertxInternal) vertx;
    this.cache = cache;
    nearCache = new ConcurrentHashMap<>();
    listener = new EntryListener();
    cache.addListener(listener);
    taskQueue = new TaskQueue();
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    vertx.getOrCreateContext().executeBlocking(fut -> {
      cache.put(new MultiMapKey(kk, vv), MeaningLessValue.INSTANCE);
      fut.complete();
    }, taskQueue, completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
    ContextImpl context = vertx.getOrCreateContext();
    @SuppressWarnings("unchecked")
    Queue<GetRequest<K, V>> getRequests = (Queue<GetRequest<K, V>>) context.contextData().computeIfAbsent(this, ctx -> new ArrayDeque<>());
    synchronized (getRequests) {
      ChoosableSet<V> entries = nearCache.get(k);
      if (entries != null && entries.isInitialised() && getRequests.isEmpty()) {
        context.runOnContext(v -> {
          resultHandler.handle(Future.succeededFuture(entries));
        });
      } else {
        getRequests.add(new GetRequest<>(k, resultHandler));
        if (getRequests.size() == 1) {
          dequeueGet(context, getRequests);
        }
      }
    }
  }

  private void dequeueGet(ContextImpl context, Queue<GetRequest<K, V>> getRequests) {
    GetRequest<K, V> getRequest;
    for (; ; ) {
      getRequest = getRequests.peek();
      ChoosableSet<V> entries = nearCache.get(getRequest.key);
      if (entries != null && entries.isInitialised()) {
        Handler<AsyncResult<ChoosableIterable<V>>> handler = getRequest.handler;
        context.runOnContext(v -> {
          handler.handle(Future.succeededFuture(entries));
        });
        getRequests.remove();
        if (getRequests.isEmpty()) {
          return;
        }
      } else {
        break;
      }
    }
    K key = getRequest.key;
    Handler<AsyncResult<ChoosableIterable<V>>> handler = getRequest.handler;
    context.<ChoosableIterable<V>>executeBlocking(fut -> {
      List<MultiMapKey> collect = cache.keySet().parallelStream()
        .filter(new KeyEqualsPredicate(DataConverter.toCachedObject(key)))
        .collect(CacheCollectors.serializableCollector(Collectors::toList));
      Collection<V> entries = collect.stream()
        .map(mmk -> DataConverter.<V>fromCachedObject(mmk.getValue()))
        .collect(toList());
      ChoosableSet<V> sids;
      if (entries != null) {
        sids = new ChoosableSet<>(entries.size());
        for (V hid : entries) {
          sids.add(hid);
        }
      } else {
        sids = new ChoosableSet<>(0);
      }
      ChoosableSet<V> prev = (sids.isEmpty()) ? null : nearCache.putIfAbsent(key, sids);
      if (prev != null) {
        // Merge them
        prev.merge(sids);
        sids = prev;
      }
      sids.setInitialised();
      fut.complete(sids);
    }, taskQueue, res -> {
      synchronized (getRequests) {
        context.runOnContext(v -> {
          handler.handle(res);
        });
        getRequests.remove();
        if (!getRequests.isEmpty()) {
          dequeueGet(context, getRequests);
        }
      }
    });
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    vertx.getOrCreateContext().executeBlocking(fut -> {
      fut.complete(cache.remove(new MultiMapKey(kk, vv), MeaningLessValue.INSTANCE));
    }, taskQueue, completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(v::equals, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    vertx.getOrCreateContext().executeBlocking(future -> {
      cache.keySet().removeIf(multiMapKey -> p.test(DataConverter.fromCachedObject(multiMapKey.getValue())));
      future.complete();
    }, taskQueue, completionHandler);

  }

  @Override
  public void close() {
    cache.removeListener(listener);
  }

  public void clearCache() {
    nearCache.clear();
  }

  @Listener(clustered = true, observation = POST)
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

  private static class GetRequest<K, V> {
    final K key;
    final Handler<AsyncResult<ChoosableIterable<V>>> handler;

    GetRequest(K key, Handler<AsyncResult<ChoosableIterable<V>>> handler) {
      this.key = key;
      this.handler = handler;
    }
  }
}
