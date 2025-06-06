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

package io.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.tests.eventbus.WrappedClusterManager;
import org.infinispan.health.Health;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;
import static org.infinispan.lifecycle.ComponentStatus.STOPPING;

/**
 * @author Thomas Segismont
 */
public class Lifecycle {

  private static final Logger log = LoggerFactory.getLogger(Lifecycle.class);

  public static void close(List<Vertx> clustered) throws Exception {
    for (Vertx vertx : clustered) {
      VertxInternal vertxInternal = (VertxInternal) vertx;

      InfinispanClusterManager clusterManager = getInfinispanClusterManager(vertxInternal.clusterManager());

      ComponentStatus status = null;
      if (clusterManager != null) {
        EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) clusterManager.getCacheContainer();
        status = cacheManager.getStatus();

        Health health = cacheManager.getHealth();

        SECONDS.sleep(2); // Make sure rebalancing has been triggered

        long start = System.currentTimeMillis();
        try {
          while (health.getClusterHealth().getHealthStatus() != HealthStatus.HEALTHY
            && System.currentTimeMillis() - start < MILLISECONDS.convert(2, MINUTES)) {
            MILLISECONDS.sleep(100);
          }
        } catch (Exception ignore) {
        }
      }

      if (status == null || status.compareTo(STOPPING) >= 0) {
        vertxInternal.close();
      } else {
        CountDownLatch latch = new CountDownLatch(1);
        vertxInternal.close().onComplete(ar -> {
          if (ar.failed()) {
            log.error("Failed to shutdown vert.x", ar.cause());
          }
          latch.countDown();
        });
        latch.await(2, TimeUnit.MINUTES);
      }
    }
  }

  private static InfinispanClusterManager getInfinispanClusterManager(ClusterManager cm) {
    if (cm == null) {
      return null;
    }
    if (cm instanceof WrappedClusterManager) {
      return getInfinispanClusterManager(((WrappedClusterManager) cm).getDelegate());
    }
    if (cm instanceof InfinispanClusterManager) {
      return (InfinispanClusterManager) cm;
    }
    throw new ClassCastException("Unexpected cluster manager implementation: " + cm.getClass());
  }

  private Lifecycle() {
    // Utility
  }
}
