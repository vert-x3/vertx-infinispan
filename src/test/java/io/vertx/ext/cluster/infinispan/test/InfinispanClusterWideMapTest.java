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

package io.vertx.ext.cluster.infinispan.test;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.cluster.infinispan.InfinispanAsyncMap;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.test.core.ClusterWideMapTestDifferentNodes;
import io.vertx.test.core.Repeat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanClusterWideMapTest extends ClusterWideMapTestDifferentNodes {

  @Override
  protected void clusteredVertx(VertxOptions options, Handler<AsyncResult<Vertx>> ar) {
    CountDownLatch latch = new CountDownLatch(1);
    Future<Vertx> future = Future.future();
    future.setHandler(ar);
    super.clusteredVertx(options, asyncResult -> {
      if (asyncResult.succeeded()) {
        future.complete(asyncResult.result());
      } else {
        future.fail(asyncResult.cause());
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
  @Repeat(times = 10)
  public void testKeyStream() {
    Map<JsonObject, Buffer> map = genJsonToBuffer(100);
    loadData(map, (vertx, asyncMap) -> {
      List<JsonObject> keys = new ArrayList<>();
      ReadStream<JsonObject> stream = InfinispanAsyncMap.<JsonObject, Buffer>unwrap(asyncMap).keyStream();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        assertEquals(map.size(), keys.size());
        assertTrue(keys.containsAll(map.keySet()));
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 3 * pause);
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      }).handler(jsonObject -> {
        keys.add(jsonObject);
        if (jsonObject.getInteger("key") == 3 || jsonObject.getInteger("key") == 16 || jsonObject.getInteger("key") == 38) {
          stream.pause();
          int emitted = keys.size();
          vertx.setTimer(pause, tid -> {
            assertTrue("Items emitted during pause", emitted == keys.size());
            stream.resume();
          });
        }
      });
    });
    await();
  }

  @Test
  @Repeat(times = 10)
  public void testValueStream() {
    Map<JsonObject, Buffer> map = genJsonToBuffer(100);
    loadData(map, (vertx, asyncMap) -> {
      List<Buffer> values = new ArrayList<>();
      ReadStream<Buffer> stream = InfinispanAsyncMap.<JsonObject, Buffer>unwrap(asyncMap).valueStream();
      AtomicInteger idx = new AtomicInteger();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        assertEquals(map.size(), values.size());
        assertTrue(values.containsAll(map.values()));
        assertTrue(map.values().containsAll(values));
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 3 * pause);
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      }).handler(buffer -> {
        values.add(buffer);
        int j = idx.getAndIncrement();
        if (j == 3 || j == 16 || j == 38) {
          stream.pause();
          int emitted = values.size();
          vertx.setTimer(pause, tid -> {
            assertTrue("Items emitted during pause", emitted == values.size());
            stream.resume();
          });
        }
      });
    });
    await();
  }

  @Test
  @Repeat(times = 10)
  public void testEntryStream() {
    Map<JsonObject, Buffer> map = genJsonToBuffer(100);
    loadData(map, (vertx, asyncMap) -> {
      List<Map.Entry<JsonObject, Buffer>> entries = new ArrayList<>();
      ReadStream<Map.Entry<JsonObject, Buffer>> stream = InfinispanAsyncMap.<JsonObject, Buffer>unwrap(asyncMap).entryStream();
      long pause = 500;
      long start = System.nanoTime();
      stream.endHandler(end -> {
        assertEquals(map.size(), entries.size());
        assertTrue(entries.containsAll(map.entrySet()));
        long duration = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(duration >= 3 * pause);
        testComplete();
      }).exceptionHandler(t -> {
        fail(t);
      }).handler(entry -> {
        entries.add(entry);
        if (entry.getKey().getInteger("key") == 3 || entry.getKey().getInteger("key") == 16 || entry.getKey().getInteger("key") == 38) {
          stream.pause();
          int emitted = entries.size();
          vertx.setTimer(pause, tid -> {
            assertTrue("Items emitted during pause", emitted == entries.size());
            stream.resume();
          });
        }
      });
    });
    await();
  }

  @Test
  @Repeat(times = 10)
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
}
