/*
 * Copyright 2023 Red Hat, Inc.
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

package io.vertx.core.eventbus;

import io.vertx.core.*;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Thomas Segismont
 */
public class InfinispanFaultToleranceTest extends FaultToleranceTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  public void setUp() throws Exception {
    System.setProperty("jgroups.file.location", temporaryFolder.newFolder().getAbsolutePath());
    super.setUp();
  }

  @Override
  protected void clusteredVertx(VertxOptions options, ClusterManager clusterManager, Handler<AsyncResult<Vertx>> handler) {
    CountDownLatch latch = new CountDownLatch(1);
    Promise<Vertx> promise = Promise.promise();
    promise.future().onComplete(ar -> {
      handler.handle(ar);
      latch.countDown();
    });
    super.clusteredVertx(options, clusterManager, promise);
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
  protected List<String> getExternalNodeSystemProperties() {
    return Arrays.asList(
      "-Djava.net.preferIPv4Stack=true",
      "-Djgroups.join_timeout=1000",
      "-Dvertx.infinispan.config=infinispan.xml",
      "-Dvertx.jgroups.config=jgroups.xml",
      "-Djgroups.file.location=" + System.getProperty("jgroups.file.location"),
      "-Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory"
    );
  }

  @Override
  protected void afterNodesKilled() throws Exception {
    super.afterNodesKilled();
    // Additionnal wait to make sure all nodes noticed the shutdowns
    Thread.sleep(30_000);
  }
}
