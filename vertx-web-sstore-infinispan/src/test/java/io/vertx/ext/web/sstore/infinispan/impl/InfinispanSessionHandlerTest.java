/*
 * Copyright 2022 Red Hat, Inc.
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

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.sstore.infinispan.InfinispanSessionStore;
import io.vertx.ext.web.tests.handler.SessionHandlerTestBase;
import io.vertx.junit5.VertxTestContext;
import org.infinispan.commons.util.Version;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DEFAULT_HOTROD_PORT;

public class InfinispanSessionHandlerTest extends SessionHandlerTestBase {

  private static final String IDENTITIES_BATCH = "/user-config/identities.batch";
  private static final String USER = "foo";
  private static final String PASS = "bar";

  public static GenericContainer<?> container =
    new GenericContainer<>("infinispan/server:" + Version.getVersion())
      .withExposedPorts(DEFAULT_HOTROD_PORT)
      .withClasspathResourceMapping("identities.batch", "/user-config/identities.batch", BindMode.READ_ONLY)
      .withEnv("IDENTITIES_BATCH", IDENTITIES_BATCH)
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Infinispan Server.*started in.*\\s"));

  @BeforeAll
  static void beforeAll() {
    container.start();
  }

  @AfterAll
  static void afterAll() {
    container.stop();
  }

  @BeforeEach
  @Override
  public void setUp(Vertx vertx, VertxTestContext testContext) throws Exception {
    JsonObject config = new JsonObject()
      .put("servers", new JsonArray().add(new JsonObject()
        .put("host", container.getHost())
        .put("port", container.getMappedPort(DEFAULT_HOTROD_PORT))
        .put("username", USER)
        .put("password", PASS)
      ));
    store = InfinispanSessionStore.create(vertx, config);
    super.setUp(vertx, testContext);
  }
}
