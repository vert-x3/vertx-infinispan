/*
 * Copyright 2021 Red Hat, Inc.
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

import io.vertx.core.Future;
import io.vertx.core.impl.VertxInternal;
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

  private final VertxInternal vertx;
  private final AdvancedCache<byte[], byte[]> cache;
  private final AdvancedCache<byte[], byte[]> ignoreReturnCache;

  public InfinispanAsyncMapImpl(VertxInternal vertx, Cache<byte[], byte[]> cache) {
    this.vertx = vertx;
    this.cache = cache.getAdvancedCache();
    ignoreReturnCache = this.cache.withFlags(Flag.IGNORE_RETURN_VALUES);
  }

  @Override
  public Future<V> get(K k) {
    byte[] kk = DataConverter.toCachedObject(k);
    return Future.fromCompletionStage(cache.getAsync(kk), vertx.getOrCreateContext())
      .map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<Void> put(K k, V v) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    return Future.fromCompletionStage(ignoreReturnCache.putAsync(kk, vv), vertx.getOrCreateContext()).mapEmpty();
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    CompletableFuture<byte[]> completionStage = ignoreReturnCache.putAsync(kk, vv, ttl, TimeUnit.MILLISECONDS);
    return Future.fromCompletionStage(completionStage, vertx.getOrCreateContext()).mapEmpty();
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    return Future.fromCompletionStage(cache.putIfAbsentAsync(kk, vv), vertx.getOrCreateContext()).map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    CompletableFuture<byte[]> completionStage = cache.putIfAbsentAsync(kk, vv, ttl, TimeUnit.MILLISECONDS);
    return Future.fromCompletionStage(completionStage, vertx.getOrCreateContext()).map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<V> remove(K k) {
    byte[] kk = DataConverter.toCachedObject(k);
    return Future.fromCompletionStage(cache.removeAsync(kk), vertx.getOrCreateContext()).map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    return Future.fromCompletionStage(cache.removeAsync(kk, vv), vertx.getOrCreateContext());
  }

  @Override
  public Future<V> replace(K k, V v) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    return Future.fromCompletionStage(cache.replaceAsync(kk, vv), vertx.getOrCreateContext()).map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<V> replace(K k, V v, long ttl) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] vv = DataConverter.toCachedObject(v);
    return Future.fromCompletionStage(cache.replaceAsync(kk, vv, ttl, TimeUnit.MILLISECONDS), vertx.getOrCreateContext()).map(DataConverter::fromCachedObject);
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] oo = DataConverter.toCachedObject(oldValue);
    byte[] nn = DataConverter.toCachedObject(newValue);
    return Future.fromCompletionStage(cache.replaceAsync(kk, oo, nn), vertx.getOrCreateContext());
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue, long ttl) {
    byte[] kk = DataConverter.toCachedObject(k);
    byte[] oo = DataConverter.toCachedObject(oldValue);
    byte[] nn = DataConverter.toCachedObject(newValue);
    return Future.fromCompletionStage(cache.replaceAsync(kk, oo, nn, ttl, TimeUnit.MILLISECONDS), vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> clear() {
    return Future.fromCompletionStage(cache.clearAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Integer> size() {
    return vertx.executeBlocking(cache::size, false);
  }

  @Override
  public Future<Set<K>> keys() {
    return vertx.executeBlocking(() -> {
      Set<byte[]> cacheKeys = cache.keySet().stream().collect(CacheCollectors.serializableCollector(Collectors::toSet));
      return cacheKeys.stream().<K>map(DataConverter::fromCachedObject).collect(toSet());
    }, false);
  }

  @Override
  public Future<List<V>> values() {
    return vertx.executeBlocking(() -> {
      List<byte[]> cacheValues = cache.values().stream().collect(CacheCollectors.serializableCollector(Collectors::toList));
      return cacheValues.stream().<V>map(DataConverter::fromCachedObject).collect(toList());
    }, false);
  }

  @Override
  public Future<Map<K, V>> entries() {
    return vertx.executeBlocking(() -> {
      Map<byte[], byte[]> cacheEntries = cache.entrySet().stream()
        .collect(CacheCollectors.serializableCollector(() -> toMap(Entry::getKey, Entry::getValue)));
      Map<K, V> result = new HashMap<>();
      for (Entry<byte[], byte[]> entry : cacheEntries.entrySet()) {
        K k = DataConverter.fromCachedObject(entry.getKey());
        V v = DataConverter.fromCachedObject(entry.getValue());
        result.put(k, v);
      }
      return result;
    }, false);
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
