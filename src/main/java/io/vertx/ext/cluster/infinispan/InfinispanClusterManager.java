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
import io.vertx.ext.cluster.infinispan.impl.InfinispanAsyncMultiMap.MultiMapKey;
import io.vertx.ext.cluster.infinispan.impl.JGroupsCounter;
import io.vertx.ext.cluster.infinispan.impl.JGroupsLock;
import org.infinispan.Cache;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jboss.marshalling.AbstractClassResolver;
import org.jgroups.blocks.atomic.CounterService;
import org.jgroups.blocks.locking.LockService;

import java.io.IOException;
import java.io.InputStream;
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

  private static final String CONFIG_TEMPLATE = "__vertx.distributed.cache.config";

  private final String configPath;

  private Vertx vertx;
  private DefaultCacheManager cacheManager;
  private NodeListener nodeListener;
  private CounterService counterService;
  private LockService lockService;
  private volatile boolean active;

  public InfinispanClusterManager() {
    this.configPath = "infinispan.xml";
  }

  public InfinispanClusterManager(String configPath) {
    Objects.requireNonNull(configPath, "configPath");
    this.configPath = configPath;
  }

  @Override
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      Cache<MultiMapKey, Object> cache = cacheManager.getCache(name, CONFIG_TEMPLATE);
      future.complete(new InfinispanAsyncMultiMap<>(vertx, cache));
    }, false, resultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      Cache<Object, Object> cache = cacheManager.getCache(name, CONFIG_TEMPLATE);
      future.complete(new InfinispanAsyncMap<>(vertx, cache));
    }, false, resultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return cacheManager.getCache(name, CONFIG_TEMPLATE);
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
  public synchronized void nodeListener(NodeListener nodeListener) {
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
      try {
        FileLookup fileLookup = FileLookupFactory.newInstance();
        InputStream inputStream = fileLookup.lookupFileStrict(configPath, Thread.currentThread().getContextClassLoader());
        ConfigurationBuilderHolder builderHolder = new ParserRegistry().parse(inputStream);
        builderHolder.getGlobalConfigurationBuilder().serialization().classResolver(new AbstractClassResolver() {
          @Override
          protected ClassLoader getClassLoader() {
            ClassLoader ctxcl = Thread.currentThread().getContextClassLoader();
            return ctxcl != null ? ctxcl : getClass().getClassLoader();
          }
        });
        cacheManager = new DefaultCacheManager(builderHolder, true);
      } catch (IOException e) {
        future.fail(e);
      }
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

  @Listener(sync = false)
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
        added.forEach(address -> {
          if (nodeListener != null) {
            nodeListener.nodeAdded(address.toString());
          }
        });
        List<Address> removed = new ArrayList<>(e.getOldMembers());
        removed.removeAll(e.getNewMembers());
        log.debug("Members removed = " + removed);
        removed.forEach(address -> {
          if (nodeListener != null) {
            nodeListener.nodeLeft(address.toString());
          }
        });
      }
    }
  }
}
