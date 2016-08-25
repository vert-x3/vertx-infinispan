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

package io.vertx.ext.cluster.infinispan;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.cluster.infinispan.impl.InfinispanAsyncMap;
import io.vertx.ext.cluster.infinispan.impl.InfinispanAsyncMultiMap;
import io.vertx.ext.cluster.infinispan.impl.JGroupsCounter;
import io.vertx.ext.cluster.infinispan.impl.JGroupsLock;
import io.vertx.ext.cluster.infinispan.impl.JGroupsLogFactory;
import io.vertx.ext.cluster.infinispan.impl.MultiMapKey;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Global;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.blocks.locking.LockService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanClusterManager implements ClusterManager {
  private static final Logger log = LoggerFactory.getLogger(InfinispanClusterManager.class);

  static {
    if (System.getProperty(Global.CUSTOM_LOG_FACTORY) == null) {
      System.setProperty(Global.CUSTOM_LOG_FACTORY, JGroupsLogFactory.class.getName());
    }
  }

  private static final String CLUSTER_MAP_NAME = "__vertx.haInfo";
  private static final String SUBS_MAP_NAME = "__vertx.subs";

  private final GlobalConfiguration globalConfiguration;

  private Vertx vertx;
  private DefaultCacheManager cacheManager;
  private NodeListener nodeListener;
  private CounterService counterService;
  private LockService lockService;
  private volatile boolean active;

  public InfinispanClusterManager() {
    this.globalConfiguration = createGlobalConfiguration(null).build();
  }

  public InfinispanClusterManager(String jgroupsConfig) {
    Objects.requireNonNull(jgroupsConfig, "jgroupsConfig");
    this.globalConfiguration = createGlobalConfiguration(jgroupsConfig).build();
  }

  public InfinispanClusterManager(GlobalConfiguration globalConfiguration) {
    Objects.requireNonNull(globalConfiguration, "globalConfiguration");
    this.globalConfiguration = globalConfiguration;
  }

  public static GlobalConfigurationBuilder createGlobalConfiguration(String jgroupsConfig) {
    GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
    globalConfigurationBuilder.transport().defaultTransport()
      .addProperty("configurationFile", jgroupsConfig == null ? "vert.x-jgroups.xml" : jgroupsConfig);
    return globalConfigurationBuilder;
  }

  @Override
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      defineConfigurationIfNeeded(name);
      Cache<MultiMapKey<Object, Object>, String> cache = cacheManager.getCache(name);
      future.complete(new InfinispanAsyncMultiMap<>(vertx, cache));
    }, false, resultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      defineConfigurationIfNeeded(name);
      Cache<Object, Object> cache = cacheManager.getCache(name);
      future.complete(new InfinispanAsyncMap<>(vertx, cache));
    }, false, resultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    defineConfigurationIfNeeded(name);
    return cacheManager.getCache(name);
  }

  private void defineConfigurationIfNeeded(String name) {
    if (cacheManager.getCacheConfiguration(name) != null) {
      return;
    }
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    switch (name) {
      case SUBS_MAP_NAME:
      case CLUSTER_MAP_NAME:
        configurationBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
        break;
      default:
        configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

    }
    Configuration configuration = configurationBuilder
      .eviction().strategy(EvictionStrategy.NONE)
      .expiration().wakeUpInterval(-1)
      .build();
    cacheManager.defineConfiguration(name, configuration);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    vertx.executeBlocking(future -> {
      java.util.concurrent.locks.Lock lock = lockService.getLock(name);
      try {
        if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
          future.complete(new JGroupsLock(vertx, lock));
        } else {
          future.fail("Timed out waiting to get lock " + name);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.fail(e);
      }
    }, false, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    vertx.executeBlocking(future -> {
      future.complete(new JGroupsCounter(vertx, counterService.getOrCreateCounter(name, 0)));
    }, false, resultHandler);
  }

  @Override
  public String getNodeID() {
    return cacheManager.getNodeAddress();
  }

  @Override
  public List<String> getNodes() {
    return cacheManager.getTransport().getMembers().stream().map(Address::toString).collect(toList());
  }

  @Override
  public void nodeListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
  }

  @Override
  public void join(Handler<AsyncResult<Void>> resultHandler) {
    vertx.executeBlocking(future -> {
      if (active) {
        future.complete();
        return;
      }
      active = true;
      cacheManager = new DefaultCacheManager(globalConfiguration);
      cacheManager.addListener(new ClusterViewListener());
      JGroupsTransport transport = (JGroupsTransport) cacheManager.getTransport();
      counterService = new CounterService(transport.getChannel());
      lockService = new LockService(transport.getChannel());
      future.complete();
    }, false, resultHandler);
  }

  @Override
  public void leave(Handler<AsyncResult<Void>> resultHandler) {
    vertx.executeBlocking(future -> {
      if (!active) {
        future.complete();
        return;
      }
      active = false;
      cacheManager.stop();
      future.complete();
    }, false, resultHandler);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Listener
  private class ClusterViewListener {
    @ViewChanged
    public void handleViewChange(final ViewChangedEvent e) {
      synchronized (InfinispanClusterManager.this) {
        if (!active) {
          return;
        }
        List<Address> added = new ArrayList<>(e.getNewMembers());
        added.removeAll(e.getOldMembers());
        log.debug("Members added = " + added);
        added.forEach(address -> nodeListener.nodeAdded(address.toString()));
        List<Address> removed = new ArrayList<>(e.getOldMembers());
        removed.removeAll(e.getNewMembers());
        log.debug("Members removed = " + removed);
        removed.forEach(address -> nodeListener.nodeLeft(address.toString()));
      }
    }
  }
}
