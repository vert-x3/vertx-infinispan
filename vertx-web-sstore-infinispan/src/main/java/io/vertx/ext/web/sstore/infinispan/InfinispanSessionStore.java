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

package io.vertx.ext.web.sstore.infinispan;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.sstore.SessionStore;
import io.vertx.ext.web.sstore.infinispan.impl.InfinispanSessionStoreImpl;

/**
 * An implementation of {@link SessionStore} that relies on the Infinispan Java Client.
 */
@VertxGen
public interface InfinispanSessionStore extends SessionStore {

  /**
   * Create a new {@link InfinispanSessionStore} for the given configuration.
   *
   * @param vertx   vertx instance
   * @param options the configuration
   * @return the new instance
   */
  static InfinispanSessionStore create(Vertx vertx, JsonObject options) {
    InfinispanSessionStore store = new InfinispanSessionStoreImpl();
    store.init(vertx, options);
    return store;
  }
}
