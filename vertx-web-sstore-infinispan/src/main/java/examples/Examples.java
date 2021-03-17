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

package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.sstore.SessionStore;
import io.vertx.ext.web.sstore.infinispan.InfinispanSessionStore;
import org.infinispan.client.hotrod.RemoteCacheManager;

@SuppressWarnings("unused")
public class Examples {

  void simpleSetup(Vertx vertx, Router router) {
    JsonObject config = new JsonObject()
      .put("servers", new JsonArray()
        .add(new JsonObject()
          .put("host", "server1.datagrid.mycorp.int")
          .put("username", "foo")
          .put("password", "bar"))
        .add(new JsonObject()
          .put("host", "server2.datagrid.mycorp.int")
          .put("username", "foo")
          .put("password", "bar"))
      );
    SessionStore store = SessionStore.create(vertx, config);
    SessionHandler sessionHandler = SessionHandler.create(store);
    router.route().handler(sessionHandler);
  }

  void explicitSetup(Vertx vertx, Router router) {
    JsonObject config = new JsonObject()
      .put("servers", new JsonArray()
        .add(new JsonObject()
          .put("host", "server1.datagrid.mycorp.int")
          .put("username", "foo")
          .put("password", "bar"))
        .add(new JsonObject()
          .put("host", "server2.datagrid.mycorp.int")
          .put("username", "foo")
          .put("password", "bar"))
      );
    InfinispanSessionStore store = InfinispanSessionStore.create(vertx, config);
    SessionHandler sessionHandler = SessionHandler.create(store);
    router.route().handler(sessionHandler);
  }

  void customClient(Vertx vertx, JsonObject config, RemoteCacheManager remoteCacheManager) {
    InfinispanSessionStore sessionStore = InfinispanSessionStore.create(vertx, config, remoteCacheManager);
  }

}
