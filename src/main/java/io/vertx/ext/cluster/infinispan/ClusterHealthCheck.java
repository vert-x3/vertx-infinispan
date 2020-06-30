/*
 * Copyright 2018 Red Hat, Inc.
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

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.cluster.infinispan.impl.ClusterHealthCheckImpl;
import io.vertx.ext.healthchecks.Status;

/**
 * A helper to create Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedures.
 *
 * @author Thomas Segismont
 */
@VertxGen
public interface ClusterHealthCheck extends Handler<Promise<Status>> {

  /**
   * Like {@link #createProcedure(Vertx, boolean)} with {@code details} set to {@code true}.
   */
  static ClusterHealthCheck createProcedure(Vertx vertx) {
    return createProcedure(vertx, true);
  }

  /**
   * Creates a ready-to-use Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedure.
   *
   * @param vertx    the instance of Vert.x, must not be {@code null}
   * @param detailed when set to {@code true}, {@link Status} data will be filled with cluster health details
   * @return a Vert.x cluster {@link io.vertx.ext.healthchecks.HealthChecks} procedure
   */
  static ClusterHealthCheck createProcedure(Vertx vertx, boolean detailed) {
    return new ClusterHealthCheckImpl(vertx, detailed);
  }
}
