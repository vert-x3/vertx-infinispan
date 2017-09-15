/*
 * Copyright 2017 Red Hat, Inc.
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

package io.vertx.ext.cluster.infinispan;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.impl.SharedDataImpl.WrappedAsyncMap;
import io.vertx.core.streams.ReadStream;

import java.util.Map;

/**
 * Extensions to the generic {@link AsyncMap}.
 *
 * @author Thomas Segismont
 */
@VertxGen
public interface InfinispanAsyncMap<K, V> {

  /**
   * Unwraps a generic {@link AsyncMap} to an {@link InfinispanAsyncMap}.
   *
   * @throws IllegalArgumentException if underlying implementation is not Infinispan
   */
  @SuppressWarnings("unchecked")
  static <K, V> InfinispanAsyncMap<K, V> unwrap(AsyncMap asyncMap) {
    if (asyncMap instanceof WrappedAsyncMap) {
      WrappedAsyncMap wrappedAsyncMap = (WrappedAsyncMap) asyncMap;
      AsyncMap delegate = wrappedAsyncMap.getDelegate();
      if (delegate instanceof InfinispanAsyncMap) {
        return (InfinispanAsyncMap<K, V>) delegate;
      }
    }
    throw new IllegalArgumentException(String.valueOf(asyncMap != null ? asyncMap.getClass() : null));
  }

  /**
   * Get the keys of the map as a {@link ReadStream}.
   * <p>
   * The stream will be automatically closed if it fails or ends.
   * Otherwise you must set a null {@link ReadStream#handler(Handler) data handler} after usage to avoid leaking resources.
   *
   * @return a stream of map keys
   */
  ReadStream<K> keyStream();

  /**
   * Get the values of the map as a {@link ReadStream}.
   * <p>
   * The stream will be automatically closed if it fails or ends.
   * Otherwise you must set a null {@link ReadStream#handler(Handler) data handler} after usage to avoid leaking resources.
   *
   * @return a stream of map values
   */
  ReadStream<V> valueStream();

  /**
   * Get the entries of the map as a {@link ReadStream}.
   * <p>
   * The stream will be automatically closed if it fails or ends.
   * Otherwise you must set a null {@link ReadStream#handler(Handler) data handler} after usage to avoid leaking resources.
   *
   * @return a stream of map entries
   */
  @GenIgnore
  ReadStream<Map.Entry<K, V>> entryStream();
}
