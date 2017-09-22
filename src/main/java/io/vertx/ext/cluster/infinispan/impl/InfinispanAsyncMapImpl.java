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
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.cluster.infinispan.InfinispanAsyncMap;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.stream.CacheCollectors;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanAsyncMapImpl<K, V> implements AsyncMap<K, V>, InfinispanAsyncMap<K, V> {

  private final Vertx vertx;
  private final AdvancedCache<Object, Object> cache;

  public InfinispanAsyncMapImpl(Vertx vertx, Cache<Object, Object> cache) {
    this.vertx = vertx;
    this.cache = cache.getAdvancedCache();
  }

  private <T> void whenComplete(CompletableFuture<T> completableFuture, Future<T> future) {
    // Context must be created in the calling thread to ensure the proper context is used
    Context context = vertx.getOrCreateContext();
    completableFuture.whenComplete((v, t) -> {
      if (t != null) {
        context.runOnContext(h -> future.fail(t));
      } else {
        context.runOnContext(h -> future.complete(v));
      }
    });
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> resultHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map(DataConverter::<V>fromCachedObject).setHandler(resultHandler);
    whenComplete(cache.getAsync(kk), vertxFuture);
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map((Void) null).setHandler(completionHandler);
    whenComplete(cache.withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(kk, vv), vertxFuture);
  }

  @Override
  public void put(K k, V v, long ttl, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map((Void) null).setHandler(completionHandler);
    whenComplete(cache.withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(kk, vv, ttl, TimeUnit.MILLISECONDS), vertxFuture);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map(DataConverter::<V>fromCachedObject).setHandler(completionHandler);
    whenComplete(cache.putIfAbsentAsync(kk, vv), vertxFuture);
  }

  @Override
  public void putIfAbsent(K k, V v, long ttl, Handler<AsyncResult<V>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map(DataConverter::<V>fromCachedObject).setHandler(completionHandler);
    whenComplete(cache.putIfAbsentAsync(kk, vv, ttl, TimeUnit.MILLISECONDS), vertxFuture);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> resultHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map(DataConverter::<V>fromCachedObject).setHandler(resultHandler);
    whenComplete(cache.removeAsync(kk), vertxFuture);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Boolean> vertxFuture = Future.future();
    vertxFuture.setHandler(resultHandler);
    whenComplete(cache.removeAsync(kk, vv), vertxFuture);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> resultHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Future<Object> vertxFuture = Future.future();
    vertxFuture.map(DataConverter::<V>fromCachedObject).setHandler(resultHandler);
    whenComplete(cache.replaceAsync(kk, vv), vertxFuture);
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object oo = DataConverter.toCachedObject(oldValue);
    Object nn = DataConverter.toCachedObject(newValue);
    Future<Boolean> vertxFuture = Future.future();
    vertxFuture.setHandler(resultHandler);
    whenComplete(cache.replaceAsync(kk, oo, nn), vertxFuture);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    Future<Void> vertxFuture = Future.future();
    vertxFuture.setHandler(resultHandler);
    whenComplete(cache.clearAsync(), vertxFuture);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    vertx.executeBlocking(future -> future.complete(cache.size()), false, resultHandler);
  }

  @Override
  public void keys(Handler<AsyncResult<Set<K>>> resultHandler) {
    vertx.executeBlocking(future -> {
      Set<Object> cacheKeys = cache.keySet().stream().collect(CacheCollectors.serializableCollector(() -> toSet()));
      future.complete(cacheKeys.stream().<K>map(DataConverter::fromCachedObject).collect(Collectors.toSet()));
    }, false, resultHandler);
  }

  @Override
  public void values(Handler<AsyncResult<List<V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      List<Object> cacheValues = cache.values().stream().collect(CacheCollectors.serializableCollector(() -> toList()));
      future.complete(cacheValues.stream().<V>map(DataConverter::fromCachedObject).collect(Collectors.toList()));
    }, false, resultHandler);
  }

  @Override
  public void entries(Handler<AsyncResult<Map<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      Map<Object, Object> cacheEntries = cache.entrySet().stream()
        .collect(CacheCollectors.serializableCollector(() -> toMap(Entry::getKey, Entry::getValue)));
      Map<K, V> result = new HashMap<>();
      for (Entry<Object, Object> entry : cacheEntries.entrySet()) {
        K k = DataConverter.fromCachedObject(entry.getKey());
        V v = DataConverter.fromCachedObject(entry.getValue());
        result.put(k, v);
      }
      future.complete(result);
    }, false, resultHandler);
  }

  @Override
  public ReadStream<K> keyStream() {
    return new CloseableIteratorCollectionStream<>(vertx.getOrCreateContext(), cache::keySet, DataConverter::fromCachedObject);
  }

  @Override
  public ReadStream<V> valueStream() {
    return new CloseableIteratorCollectionStream<>(vertx.getOrCreateContext(), cache::values, DataConverter::fromCachedObject);
  }

  @Override
  public ReadStream<Entry<K, V>> entryStream() {
    return new CloseableIteratorCollectionStream<>(vertx.getOrCreateContext(), cache::entrySet, cacheEntry -> {
      K key = DataConverter.fromCachedObject(cacheEntry.getKey());
      V value = DataConverter.fromCachedObject(cacheEntry.getValue());
      return new SimpleImmutableEntry<>(key, value);
    });
  }
}
