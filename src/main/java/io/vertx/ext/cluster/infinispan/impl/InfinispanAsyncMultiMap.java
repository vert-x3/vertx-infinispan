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
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.infinispan.Cache;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.stream.CacheCollectors;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Thomas Segismont
 */
public class InfinispanAsyncMultiMap<K, V> implements AsyncMultiMap<K, V> {
  private static final String VALUE = "__vertx.infinispan.multimap.value";

  private final Vertx vertx;
  private final Cache<MultiMapKey<Object, Object>, String> cache;
  private final ConcurrentMap<K, ChoosableIterable<V>> choosableIterables;

  public InfinispanAsyncMultiMap(Vertx vertx, Cache<MultiMapKey<Object, Object>, String> cache) {
    this.vertx = vertx;
    this.cache = cache;
    this.choosableIterables = new ConcurrentHashMap<>();
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Context context = vertx.getOrCreateContext();
    FutureAdapter<String> futureAdapter = new FutureAdapter<>(context);
    cache.putAsync(new MultiMapKey<>(kk, vv), VALUE).attachListener(futureAdapter);
    futureAdapter.getVertxFuture().map((Void) null).setHandler(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
    ChoosableIterable<V> choosableIterable = choosableIterables.computeIfAbsent(k, ChoosableIterableImpl::new);
    Future.succeededFuture(choosableIterable).setHandler(asyncResultHandler);
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    Object kk = DataConverter.toCachedObject(k);
    Object vv = DataConverter.toCachedObject(v);
    Context context = vertx.getOrCreateContext();
    FutureAdapter<Boolean> futureAdapter = new FutureAdapter<>(context);
    cache.removeAsync(new MultiMapKey<>(kk, vv), VALUE).attachListener(futureAdapter);
    futureAdapter.getVertxFuture().setHandler(completionHandler);
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    Object vv = DataConverter.toCachedObject(v);
    vertx.executeBlocking(future -> {
      for (Iterator<MultiMapKey<Object, Object>> iterator = cache.keySet().iterator(); iterator.hasNext(); ) {
        MultiMapKey<Object, Object> next = iterator.next();
        if (next.getValue().equals(vv)) {
          iterator.remove();
        }
      }
      future.complete();
    }, false, completionHandler);
  }

  private class ChoosableIterableImpl implements ChoosableIterable<V> {
    private final Object kk;
    private volatile int idx;

    ChoosableIterableImpl(K k) {
      this.kk = DataConverter.toCachedObject(k);
    }

    @Override
    public boolean isEmpty() {
      return !cache.keySet().parallelStream().anyMatch(new KeyEqualsPredicate(kk));
    }

    @Override
    public V choose() {
      List<V> list = getList();
      return list.get(mod(idx++, list.size()));
    }

    private int mod(int idx, int size) {
      int i = idx % size;
      return i < 0 ? i + size : i;
    }

    @Override
    public Iterator<V> iterator() {
      return getList().iterator();
    }

    private List<V> getList() {
      return cache.keySet().parallelStream()
        .filter(new KeyEqualsPredicate(kk))
        .map((Serializable & Function<MultiMapKey<Object, Object>, Object>) MultiMapKey::getValue)
        .map((Serializable & Function<Object, V>) DataConverter::fromCachedObject)
        .collect(CacheCollectors.serializableCollector(Collectors::toList));
    }
  }

  @SerializeWith(KeyEqualsPredicate.KeyEqualsPredicateExternalizer.class)
  public static class KeyEqualsPredicate implements Predicate<MultiMapKey<Object, Object>> {

    private final Object kk;

    public KeyEqualsPredicate(Object kk) {
      this.kk = kk;
    }

    @Override
    public boolean test(MultiMapKey<Object, Object> mmk) {
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
}
