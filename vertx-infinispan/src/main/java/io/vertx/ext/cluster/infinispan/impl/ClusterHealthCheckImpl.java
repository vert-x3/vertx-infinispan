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

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.cluster.infinispan.ClusterHealthCheck;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.ext.healthchecks.Status;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;

import java.util.List;

/**
 * @author Thomas Segismont
 */
public class ClusterHealthCheckImpl implements ClusterHealthCheck {

  private final Vertx vertx;
  private final boolean detailed;

  public ClusterHealthCheckImpl(Vertx vertx, boolean detailed) {
    this.vertx = vertx;
    this.detailed = detailed;
  }

  @Override
  public void handle(Promise<Status> promise) {
    VertxInternal vertxInternal = (VertxInternal) vertx;
    InfinispanClusterManager clusterManager = (InfinispanClusterManager) vertxInternal.clusterManager();
    EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) clusterManager.getCacheContainer();
    Health health = cacheManager.getHealth();
    HealthStatus healthStatus = health.getClusterHealth().getHealthStatus();
    Status status = new Status().setOk(healthStatus == HealthStatus.HEALTHY);
    if (detailed) {
      status.setData(convert(health));
    }
    promise.complete(status);
  }

  private JsonObject convert(Health health) {
    return new JsonObject()
      .put("clusterHealth", convert(health.getClusterHealth()))
      .put("cacheHealth", convert(health.getCacheHealth()));
  }

  private JsonObject convert(ClusterHealth clusterHealth) {
    return new JsonObject()
      .put("healthStatus", clusterHealth.getHealthStatus().name())
      .put("clusterName", clusterHealth.getClusterName())
      .put("numberOfNodes", clusterHealth.getNumberOfNodes())
      .put("nodeNames", clusterHealth.getNodeNames().stream()
        .collect(JsonArray::new, JsonArray::add, JsonArray::addAll));
  }

  private JsonArray convert(List<CacheHealth> cacheHealths) {
    return cacheHealths.stream()
      .map(cacheHealth -> new JsonObject()
        .put("cacheName", cacheHealth.getCacheName())
        .put("status", cacheHealth.getStatus().name()))
      .collect(JsonArray::new, JsonArray::add, JsonArray::addAll);
  }
}
