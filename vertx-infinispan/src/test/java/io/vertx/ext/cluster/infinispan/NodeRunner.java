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

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.spi.cluster.ClusterManager;

public class NodeRunner {
  public static void main(String[] args) {
    if (args.length == 0) {
      throw new IllegalArgumentException("Node name must be provided.");
    }

    String nodeName = args[0];
    ClusterManager manager = new InfinispanClusterManager();

    Vertx.builder().withClusterManager(manager).buildClustered().onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
        vertx.eventBus().<String>consumer(nodeName)
          .handler((Message<String> msg) -> msg.reply("response from " + nodeName));

        // Send message every 2 seconds
        vertx.setPeriodic(2000, timerId -> {
          vertx.eventBus().send("node1", "msg from " + nodeName);
          System.out.println("sent msg to node1");
        });

        // Handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          System.out.println("Shutting down " + nodeName);
          vertx.close();
        }));
      }
    });
  }
}
