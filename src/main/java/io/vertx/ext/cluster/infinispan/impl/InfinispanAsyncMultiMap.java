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
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static org.infinispan.notifications.Listener.Observation.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {

  private final VertxInternal vertx;
  private final EmbeddedMultimapCache<Object, Object> multimapCache;
  private final ConcurrentMap<K, ChoosableSet<V>> nearCache;
  private final TaskQueue taskQueue;

  public InfinispanAsyncMultiMap(Vertx vertx, EmbeddedMultimapCache<Object, Object> multimapCache) {
    this.vertx = (VertxInternal) vertx;
    this.multimapCache = multimapCache;
    nearCache = new ConcurrentHashMap<>();
    Set<Class<? extends Annotation>> filterAnnotations = Stream.<Class<? extends Annotation>>builder()
      .add(CacheEntryCreated.class)
      .add(CacheEntryModified.class)
      .add(CacheEntryRemoved.class)
      .build()
      .collect(toSet());
    multimapCache.getCache()
      .addFilteredListener(new EntryListener(), new EventFilter(), new EventConverter(), filterAnnotations);
    taskQueue = new TaskQueue();
  }

  private <T> void cfGet(CompletableFuture<T> cf, Promise<T> future) {
    try {
      future.complete(cf.get());
    } catch (ExecutionException e) {
      future.fail(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.fail(e);
    }
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    vertx.getOrCreateContext().executeBlocking(fut -> {
      cfGet(multimapCache.put(kk, vv), fut);
    }, taskQueue, completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> resultHandler) {
    ContextInternal context = vertx.getOrCreateContext();
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

  private void dequeueGet(ContextInternal context, Queue<GetRequest<K, V>> getRequests) {
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
      Collection<Object> collect;
      try {
        collect = multimapCache.get(DataConverter.toCachedObject(key)).get();
      } catch (ExecutionException e) {
        fut.fail(e.getCause());
        return;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fut.fail(e);
        return;
      }
      Collection<V> entries = collect.stream()
        .map(DataConverter::<V>fromCachedObject)
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
      cfGet(multimapCache.remove(kk, vv), fut);
    }, taskQueue, completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(v::equals, completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
    vertx.getOrCreateContext().executeBlocking(fut -> {
      cfGet(multimapCache.remove(o -> p.test(DataConverter.fromCachedObject(o))), fut);
    }, taskQueue, completionHandler);

  }

  public void clearCache() {
    nearCache.clear();
  }

  @Listener(clustered = true, observation = POST)
  private class EntryListener {
    @CacheEntryCreated
    public void entryCreated(CacheEntryCreatedEvent<Object, Object> event) {
      K k = DataConverter.fromCachedObject(event.getKey());
      Collection values = (Collection) event.getValue();
      ChoosableSet<V> entries = nearCache.compute(k, (key, choosableSet) -> {
        return choosableSet == null ? new ChoosableSet<>(values.size()) : choosableSet;
      });
      for (Object value : values) {
        entries.add(DataConverter.fromCachedObject(value));
      }
    }

    @CacheEntryModified
    public void entryModified(CacheEntryModifiedEvent<Object, Object> event) {
      K k = DataConverter.fromCachedObject(event.getKey());
      ModifiedCollection modifiedCollection = (ModifiedCollection) event.getValue();
      ChoosableSet<V> entries = nearCache.get(k);
      if (entries != null) {
        forEachModified(modifiedCollection.toAdd, entries::add);
        forEachModified(modifiedCollection.toDelete, entries::remove);
      }
    }

    private void forEachModified(Collection<Object> collection, Consumer<V> action) {
      if (collection != null) {
        collection.stream()
          .<V>map(DataConverter::fromCachedObject)
          .forEach(action);
      }
    }

    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent<Object, Object> event) {
      K k = DataConverter.fromCachedObject(event.getKey());
      nearCache.remove(k);
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

  @SerializeWith(EventFilterExternalizer.class)
  private static class EventFilter implements CacheEventFilter<Object, Collection<Object>> {
    @Override
    public boolean accept(Object key, Collection<Object> oldValue, Metadata oldMetadata, Collection<Object> newValue, Metadata newMetadata, EventType eventType) {
      return true;
    }
  }

  public static class EventFilterExternalizer implements Externalizer<EventFilter> {

    @Override
    public void writeObject(ObjectOutput objectOutput, EventFilter eventFilter) {

    }

    @Override
    public EventFilter readObject(ObjectInput objectInput) {
      return new EventFilter();
    }
  }

  @SerializeWith(EventConverterExternalizer.class)
  private static class EventConverter implements CacheEventConverter<Object, Collection<Object>, Object> {
    @Override
    public Object convert(Object key, Collection<Object> oldValue, Metadata oldMetadata, Collection<Object> newValue, Metadata newMetadata, EventType eventType) {
      if (eventType.getType() == Event.Type.CACHE_ENTRY_MODIFIED) {
        if (oldValue != null && newValue != null) {
          oldValue.removeAll(newValue);
          newValue.removeAll(oldValue);
        }
        return new ModifiedCollection(oldValue, newValue);
      }
      return newValue;
    }
  }

  public static class EventConverterExternalizer implements Externalizer<EventConverter> {

    @Override
    public void writeObject(ObjectOutput objectOutput, EventConverter eventConverter) {
    }

    @Override
    public EventConverter readObject(ObjectInput objectInput) {
      return new EventConverter();
    }
  }

  @SerializeWith(ModifiedCollectionExternalizer.class)
  private static class ModifiedCollection {
    final Collection<Object> toDelete;
    final Collection<Object> toAdd;

    private ModifiedCollection(Collection<Object> toDelete, Collection<Object> toAdd) {
      this.toDelete = toDelete;
      this.toAdd = toAdd;
    }
  }

  public static class ModifiedCollectionExternalizer implements Externalizer<ModifiedCollection> {

    @Override
    public void writeObject(ObjectOutput objectOutput, ModifiedCollection modifiedCollection) throws IOException {
      objectOutput.writeObject(modifiedCollection.toDelete);
      objectOutput.writeObject(modifiedCollection.toAdd);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ModifiedCollection readObject(ObjectInput objectInput) throws IOException, ClassNotFoundException {
      return new ModifiedCollection((Collection<Object>) objectInput.readObject(), (Collection<Object>) objectInput.readObject());
    }
  }
}
