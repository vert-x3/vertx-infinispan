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

package io.vertx.core.shareddata;

import io.vertx.Lifecycle;
import io.vertx.LoggingTestWatcher;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.cluster.infinispan.InfinispanAsyncMap;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Thomas Segismont
 */
public class InfinispanClusteredAsyncMapTest extends ClusteredAsyncMapTest {

  @Rule
  public LoggingTestWatcher watchman = new LoggingTestWatcher();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  public void setUp() throws Exception {
    System.setProperty("jgroups.file.location", temporaryFolder.newFolder().getAbsolutePath());
    super.setUp();
  }

  @Override
  protected void clusteredVertx(VertxOptions options, Handler<AsyncResult<Vertx>> ar) {
    CountDownLatch latch = new CountDownLatch(1);
    Promise<Vertx> promise = Promise.promise();
    promise.future().onComplete(ar);
    super.clusteredVertx(options, asyncResult -> {
      if (asyncResult.succeeded()) {
        promise.complete(asyncResult.result());
      } else {
        promise.fail(asyncResult.cause());
      }
      latch.countDown();
    });
    try {
      assertTrue(latch.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new InfinispanClusterManager();
  }

  @Test
  public void testKeyStream() {
    testReadStream(InfinispanAsyncMap::keyStream, (map, keys) -> {
      assertEquals(map.size(), keys.size());
      assertTrue(keys.containsAll(map.keySet()));
    });
  }

  @Test
  public void testValueStream() {
    testReadStream(InfinispanAsyncMap::valueStream, (map, values) -> {
      assertEquals(map.size(), values.size());
      assertTrue(values.containsAll(map.values()));
      assertTrue(map.values().containsAll(values));
    });
  }

  @Test
  public void testEntryStream() {
    testReadStream(InfinispanAsyncMap::entryStream, (map, entries) -> {
      assertEquals(map.size(), entries.size());
      assertTrue(entries.containsAll(map.entrySet()));
    });
  }

  private <T> void testReadStream(
    Function<InfinispanAsyncMap<JsonObject, Buffer>, ReadStream<T>> streamFactory,
    BiConsumer<Map<JsonObject, Buffer>, List<T>> assertions
  ) {
    Map<JsonObject, Buffer> map = genJsonToBuffer(100);
    loadData(map, (vertx, asyncMap) -> {
      List<T> items = new ArrayList<>();
      ReadStream<T> stream = streamFactory.apply(InfinispanAsyncMap.unwrap(asyncMap));
      AtomicInteger idx = new AtomicInteger();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        assertions.accept(map, items);
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 3 * pause);
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      }).handler(item -> {
        items.add(item);
        int j = idx.getAndIncrement();
        if (j == 3 || j == 16 || j == 38) {
          stream.pause();
          int emitted = items.size();
          vertx.setTimer(pause, tid -> {
            assertTrue("Items emitted during pause", emitted == items.size());
            stream.resume();
          });
        }
      });
      stream.fetch(1);
    });
    await();
  }

  @Test
  public void testClosedKeyStream() {
    Map<JsonObject, Buffer> map = genJsonToBuffer(100);
    loadData(map, (vertx, asyncMap) -> {
      List<JsonObject> keys = new ArrayList<>();
      ReadStream<JsonObject> stream = InfinispanAsyncMap.<JsonObject, Buffer>unwrap(asyncMap).keyStream();
      stream.exceptionHandler(t -> {
        fail(t);
      }).handler(jsonObject -> {
        keys.add(jsonObject);
        if (jsonObject.getInteger("key") == 38) {
          stream.handler(null);
          int emitted = keys.size();
          vertx.setTimer(500, tid -> {
            assertTrue("Items emitted after close", emitted == keys.size());
            testComplete();
          });
        }
      });
    });
    await();
  }

  @Override
  protected void closeClustered(List<Vertx> clustered) throws Exception {
    Lifecycle.closeClustered(clustered);
  }
}
