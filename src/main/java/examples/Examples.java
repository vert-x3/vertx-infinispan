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

package examples;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.cluster.infinispan.ClusterHealthCheck;
import io.vertx.ext.cluster.infinispan.InfinispanAsyncMap;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.HealthChecks;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.Router;
import org.infinispan.manager.DefaultCacheManager;

import java.util.Map;

/**
 * @author Thomas Segismont
 */
public class Examples {

  public void createClusterManagerProgramatically() {
    ClusterManager mgr = new InfinispanClusterManager();

    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void useExistingCacheManager(DefaultCacheManager cacheManager) {
    ClusterManager mgr = new InfinispanClusterManager(cacheManager);

    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public <K, V> void asyncMapStreams(AsyncMap<K, V> asyncMap) {
    InfinispanAsyncMap<K, V> infinispanAsyncMap = InfinispanAsyncMap.unwrap(asyncMap);
    ReadStream<K> keyStream = infinispanAsyncMap.keyStream();
    ReadStream<V> valueStream = infinispanAsyncMap.valueStream();
    ReadStream<Map.Entry<K, V>> entryReadStream = infinispanAsyncMap.entryStream();
  }

  public void healthCheck(Vertx vertx) {
    Handler<Future<Status>> procedure = ClusterHealthCheck.createProcedure(vertx, true);
    HealthChecks checks = HealthChecks.create(vertx).register("cluster-health", procedure);
  }

  public void healthCheckHandler(Vertx vertx, HealthChecks checks) {
    Router router = Router.router(vertx);
    router.get("/readiness").handler(HealthCheckHandler.createWithHealthChecks(checks));
  }
}
