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
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.test.core.ComplexHATest;
import org.infinispan.health.Health;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanComplexHATest extends ComplexHATest {

  private static final Logger log = LoggerFactory.getLogger(InfinispanComplexHATest.class);

  @Override
  public void setUp() throws Exception {
    Random random = new Random();
    System.setProperty("vertx.infinispan.test.auth.token", new BigInteger(128, random).toString(32));
    super.setUp();
  }

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

  @Override
  protected void closeClustered(List<Vertx> clustered) throws Exception {
    for (Vertx clusteredVertx : clustered) {
      VertxInternal vertxInternal = (VertxInternal) clusteredVertx;
      InfinispanClusterManager clusterManager = (InfinispanClusterManager) vertxInternal.getClusterManager();
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) clusterManager.getCacheContainer();
      Health health = cacheManager.getHealth();
      long start = System.currentTimeMillis();
      try {
        while (health.getClusterHealth().getHealthStatus() != HealthStatus.HEALTHY
          && System.currentTimeMillis() - start < MILLISECONDS.convert(2, MINUTES)) {
          MILLISECONDS.sleep(100);
        }
      } catch (Exception ignore) {
      }
      CountDownLatch latch = new CountDownLatch(1);
      vertxInternal.close(ar -> {
        if (ar.failed()) {
          log.error("Failed to shutdown vert.x", ar.cause());
        }
        latch.countDown();
      });
      latch.await(2, TimeUnit.MINUTES);
    }
  }
}
