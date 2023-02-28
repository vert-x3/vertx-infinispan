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

package io.vertx.ext.cluster.infinispan;

import io.vertx.Lifecycle;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.infinispan.health.HealthStatus.HEALTHY;
import static org.infinispan.health.HealthStatus.HEALTHY_REBALANCING;

public class ClusterHealthCheckTest extends VertxTestBase {

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

  @Override
  protected void close(List<Vertx> clustered) throws Exception {
    Lifecycle.close(clustered);
  }

  @Test
  public void testDetailedHealthCheck() {
    startNodes(2);
    ClusterHealthCheck healthCheck = ClusterHealthCheck.createProcedure(vertices[1], true);
    vertices[0].sharedData().getAsyncMap("foo", onSuccess(asyncMap -> {
      vertices[1].executeBlocking(healthCheck, onSuccess(status -> {
        JsonObject json = new JsonObject(status.toJson().encode()); // test serialization+deserialization
        assertTrue(json.getBoolean("ok"));
        assertEquals(Integer.valueOf(2), json.getJsonObject("data").getJsonObject("clusterHealth").getInteger("numberOfNodes"));
        String fooCacheHealth = json.getJsonObject("data").getJsonArray("cacheHealth").stream()
          .map(JsonObject.class::cast)
          .filter(entry -> entry.getString("cacheName").equals("foo"))
          .findAny()
          .map(jsonObject -> jsonObject.getString("status"))
          .orElseThrow(() -> new AssertionError("foo cache is missing"));
        assertThat(fooCacheHealth, anyOf(is(HEALTHY.name()), is(HEALTHY_REBALANCING.name())));
        testComplete();
      }));
    }));
    await();
  }
}
