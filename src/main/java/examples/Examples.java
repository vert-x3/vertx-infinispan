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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.infinispan.manager.DefaultCacheManager;

/**
 * @author Thomas Segismont
 */
public class Examples {

  public void example1() {

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

  public void example2() {

    ClusterManager mgr = new InfinispanClusterManager("custom-infinispan.xml");

    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example3(DefaultCacheManager cacheManager) {
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
}
