/*
 * Copyright 2024 Red Hat, Inc.
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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZeroCapacityNodeTest extends VertxTestBase {
  private static final Logger log = LoggerFactory.getLogger(ZeroCapacityNodeTest.class);

  @Override
  public void setUp() throws Exception {
    super.setUp();
    vertices = new Vertx[2];
  }

  @Test
  public void test() throws Exception {
    Process node2 = startNode(2);

    CountDownLatch deployNode1Latch = new CountDownLatch(1);
    // Start first node with custom config
    System.setProperty("vertx.infinispan.config", "infinispan1.xml");
    System.setProperty("jgroups.tcp.port", "7800");

    ClusterManager manager1 = new InfinispanClusterManager();

    Handler<AsyncResult<Vertx>> ar = (res) -> {
      log.info("Started for node1");
      if (res.succeeded()) {
        vertices[0] = res.result();
        deployNode1Latch.countDown();
        // vertices[0].setPeriodic(5000, (v) -> printInfo(vertices[0]));
      }
    };

    clusteredVertx(new VertxOptions(), manager1).onComplete(ar);

    try {
      boolean completed = deployNode1Latch.await(60, TimeUnit.SECONDS);
      if (!completed) {
        assert node2 != null;
        node2.destroy();
        fail("Cant start node1");
      }
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }

    // wait 3 messages and succeed
    CountDownLatch counter = new CountDownLatch(3);
    vertices[0].eventBus().<String>consumer("node1").handler((Message<String> msg) -> {
      log.info("node1" + ": " + msg.body());
      msg.reply("response from node1");
      assertEquals("msg from node2", msg.body());
      counter.countDown();
    });

    try {
      assertTrue(counter.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      fail(e.getMessage());
    } finally {
      assert node2 != null;
      node2.destroy();
      Vertx vertex = vertices[0];
      close(vertex);
    }

  }

  //  public static void printInfo(Vertx vertx) {
  //    ClusterManager clusterManager = ((VertxImpl) vertx).clusterManager();
  //
  //    Map<String, byte[]> nodeInfo = clusterManager.getSyncMap("__vertx.nodeInfo");
  //    List<NodeInfo> cachedObjects = new ArrayList<>();
  //
  //    for (Object key : nodeInfo.keySet()) {
  //      NodeInfo info = DataConverter.fromCachedObject(nodeInfo.get(key));
  //      cachedObjects.add(info);
  //    }
  //
  //    List<JsonObject> listNodes = cachedObjects.stream()
  //      .map(cachedObject -> JsonObject.of("host", cachedObject.host(), "port", cachedObject.port()))
  //      .collect(Collectors.toList());
  //
  //    System.out.println(clusterManager.getNodeId() + ": " + listNodes);
  //  }

  private static Process startNode(int nodeId) {
    // Create command to run each node in a new JVM process with its specific configuration
    String config = (nodeId == 1) ? "infinispan1.xml" : "infinispan2.xml";
    String nodeName = (nodeId == 1) ? "node1" : "node2";
    String port = (nodeId == 1) ? "7800" : "7801";
    System.setProperty("jgroups.tcp.port", port);

    ProcessBuilder processBuilder =
      new ProcessBuilder("java", "-Dvertx.infinispan.config=" + config, "-Djgroups.tcp.port=" + port, "-cp",
        System.getProperty("java.class.path"), "io.vertx.ext.cluster.infinispan.NodeRunner", nodeName);

    // Start the process and wait for it to finish
    try {
      Process process = processBuilder.start();
      // Inside startNode(), within the threads:
      new Thread(() -> {
        try (InputStream inputStream = process.getInputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.out.println(line);
          }
        } catch (IOException e) {
          // Don't just printStackTrace in tests, might indicate a problem
          System.err.println("Error reading process stdout: " + e.getMessage());
        }
      }).start();
      // Similarly for the error stream...
      new Thread(() -> {
        try (InputStream inputStream = process.getErrorStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
          String line;
          while ((line = reader.readLine()) != null) {
            System.err.println(line); // Prefix helps identify source
          }
        } catch (IOException e) {
          System.err.println("Error reading process stderr: " + e.getMessage());
        }
      }).start();

      return process;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
