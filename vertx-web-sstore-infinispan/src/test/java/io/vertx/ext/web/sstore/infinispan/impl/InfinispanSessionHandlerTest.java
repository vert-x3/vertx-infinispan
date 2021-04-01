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

package io.vertx.ext.web.sstore.infinispan.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.SessionHandlerTestBase;
import io.vertx.ext.web.sstore.infinispan.InfinispanSessionStore;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DEFAULT_HOTROD_PORT;

public class InfinispanSessionHandlerTest extends SessionHandlerTestBase {

  private static final String USER = "foo";
  private static final String PASS = "bar";

  @ClassRule
  public static GenericContainer<?> container =
    new GenericContainer<>("infinispan/server:12.1")
      .withExposedPorts(DEFAULT_HOTROD_PORT)
      .withEnv("USER", USER)
      .withEnv("PASS", PASS)
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Infinispan Server.*started in.*\\s"));

  @Override
  public void setUp() throws Exception {
    super.setUp();
    JsonObject config = new JsonObject()
      .put("servers", new JsonArray().add(new JsonObject()
        .put("host", container.getHost())
        .put("port", container.getMappedPort(DEFAULT_HOTROD_PORT))
        .put("username", USER)
        .put("password", PASS)
      ));
    store = InfinispanSessionStore.create(vertx, config);
  }
}
