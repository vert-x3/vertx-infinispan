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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.prng.VertxContextPRNG;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.sstore.AbstractSession;
import io.vertx.ext.web.sstore.SessionStore;
import io.vertx.ext.web.sstore.impl.SharedDataSessionImpl;
import io.vertx.ext.web.sstore.infinispan.InfinispanSessionStore;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.SaslQop;
import org.infinispan.client.hotrod.configuration.ServerConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.vertx.ext.web.sstore.ClusteredSessionStore.DEFAULT_RETRY_TIMEOUT;
import static io.vertx.ext.web.sstore.ClusteredSessionStore.DEFAULT_SESSION_MAP_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.client.hotrod.impl.ConfigurationProperties.DEFAULT_HOTROD_PORT;

public class InfinispanSessionStoreImpl implements InfinispanSessionStore {

  private VertxInternal vertx;
  private JsonObject options;
  private VertxContextPRNG random;
  private RemoteCacheManager remoteCacheManager;
  private RemoteCache<String, byte[]> sessions;

  public InfinispanSessionStoreImpl() {
    // Required by service loader
  }

  @Override
  public SessionStore init(Vertx vertx, JsonObject options) {
    return init(vertx, options, null);
  }

  public SessionStore init(Vertx vertx, JsonObject options, RemoteCacheManager remoteCacheManager) {
    this.vertx = (VertxInternal) vertx;
    this.options = Objects.requireNonNull(options, "options are required");
    random = VertxContextPRNG.current(vertx);
    if (remoteCacheManager != null) {
      this.remoteCacheManager = remoteCacheManager;
    } else {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JsonArray servers = Objects.requireNonNull(options.getJsonArray("servers"), "servers list is required");
      for (Object object : servers) {
        if (object instanceof JsonObject) {
          JsonObject server = (JsonObject) object;
          configure(builder.addServer(), server);
        }
      }
      this.remoteCacheManager = new RemoteCacheManager(builder.build());
    }
    String cacheName = options.getString("cacheName", DEFAULT_SESSION_MAP_NAME);
    sessions = this.remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
      .getOrCreateCache(cacheName, new StringConfiguration("{\"distributed-cache\":{\"mode\":\"SYNC\",\"statistics\":\"true\"}}"));
    return this;
  }

  private static void configure(ServerConfigurationBuilder builder, JsonObject server) {
    String uri = server.getString("uri");
    if (uri != null) {
      builder.uri(uri);
    } else {
      builder
        .host(server.getString("host", "localhost"))
        .port(server.getInteger("port", DEFAULT_HOTROD_PORT));
      String clientIntelligence = server.getString("clientIntelligence");
      if (clientIntelligence != null) {
        builder.clientIntelligence(ClientIntelligence.valueOf(clientIntelligence));
      }
      builder.security().authentication()
        .username(Objects.requireNonNull(server.getString("username"), "username is required"))
        .password(Objects.requireNonNull(server.getString("password"), "password is required"))
        .realm(server.getString("realm", "default"))
        .saslMechanism(server.getString("saslMechanism", "DIGEST-MD5"));
      String saslQop = server.getString("saslQop");
      if (saslQop != null) {
        builder.security().authentication()
          .saslQop(SaslQop.valueOf(saslQop));
      }
    }
  }

  @Override
  public long retryTimeout() {
    return options.getLong("retryTimeout", DEFAULT_RETRY_TIMEOUT);
  }

  @Override
  public Session createSession(long timeout) {
    return createSession(timeout, DEFAULT_SESSIONID_LENGTH);
  }

  @Override
  public Session createSession(long timeout, int length) {
    return new SharedDataSessionImpl(random, timeout, length);
  }

  @Override
  public Future<Session> get(String id) {
    return Future.fromCompletionStage(sessions.getAsync(id), vertx.getOrCreateContext()).map(current -> {
      SharedDataSessionImpl session;
      if (current == null) {
        session = null;
      } else {
        session = new SharedDataSessionImpl(random);
        session.readFromBuffer(0, Buffer.buffer(current));
      }
      return session;
    });
  }

  @Override
  public Future<Void> delete(String id) {
    return Future.fromCompletionStage(sessions.removeAsync(id), vertx.getOrCreateContext()).mapEmpty();
  }

  @Override
  public Future<Void> put(Session session) {
    return Future.fromCompletionStage(sessions.getAsync(session.id()), vertx.getOrCreateContext()).compose(current -> {
      AbstractSession newSession = (AbstractSession) session;
      if (current != null) {
        // Old session exists, we need to validate versions
        SharedDataSessionImpl oldSession = new SharedDataSessionImpl(random);
        oldSession.readFromBuffer(0, Buffer.buffer(current));

        if (oldSession.version() != newSession.version()) {
          return Future.failedFuture("Session version mismatch");
        }
      }

      newSession.incrementVersion();
      return writeSession(newSession);
    });
  }

  private Future<Void> writeSession(Session session) {
    Buffer buffer = Buffer.buffer();
    SharedDataSessionImpl sessionImpl = (SharedDataSessionImpl) session;
    sessionImpl.writeToBuffer(buffer);

    CompletableFuture<byte[]> putAsync = sessions.putAsync(session.id(), buffer.getBytes(), 2 * session.timeout(), MILLISECONDS, session.timeout(), MILLISECONDS);
    return Future.fromCompletionStage(putAsync, vertx.getOrCreateContext()).mapEmpty();
  }

  @Override
  public Future<Void> clear() {
    return Future.fromCompletionStage(sessions.clearAsync(), vertx.getOrCreateContext());
  }

  @Override
  public Future<Integer> size() {
    return Future.fromCompletionStage(sessions.sizeAsync(), vertx.getOrCreateContext())
      .map(val -> (int) Math.min(val, Integer.MAX_VALUE));
  }

  @Override
  public void close() {
    remoteCacheManager.close();
  }
}
