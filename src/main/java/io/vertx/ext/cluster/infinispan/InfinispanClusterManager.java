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
import io.vertx.core.VertxException;
import io.vertx.core.impl.ContextImpl;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.cluster.infinispan.impl.InfinispanAsyncMapImpl;
import io.vertx.ext.cluster.infinispan.impl.InfinispanAsyncMultiMap;
import io.vertx.ext.cluster.infinispan.impl.InfinispanCounter;
import io.vertx.ext.cluster.infinispan.impl.JGroupsLock;
import org.infinispan.Cache;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.multimap.api.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.MultimapCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.JChannel;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.*;
import static org.jgroups.stack.ProtocolStack.Position.*;

/**
 * @author Thomas Segismont
 */
public class InfinispanClusterManager implements ClusterManager {
  private static final Logger log = LoggerFactory.getLogger(InfinispanClusterManager.class);

  private static final String VERTX_INFINISPAN_CONFIG_PROP_NAME = "vertx.infinispan.config";
  private static final String INFINISPAN_XML = "infinispan.xml";
  private static final String DEFAULT_INFINISPAN_XML = "default-infinispan.xml";
  private static final String VERTX_JGROUPS_CONFIG_PROP_NAME = "vertx.jgroups.config";
  private static final String JGROUPS_XML = "jgroups.xml";

  private final String ispnConfigPath;
  private final String jgroupsConfigPath;
  private final boolean userProvidedCacheManager;

  private Vertx vertx;
  private DefaultCacheManager cacheManager;
  private NodeListener nodeListener;
  private LockService lockService;
  private volatile boolean active;
  private ClusterViewListener viewListener;
  // Guarded by this
  private Set<InfinispanAsyncMultiMap> multimaps = Collections.newSetFromMap(new WeakHashMap<>(1));
  private ForkChannel forkChannel;

  /**
   * Creates a new cluster manager configured with {@code infinispan.xml} and {@code jgroups.xml} files.
   */
  public InfinispanClusterManager() {
    this.ispnConfigPath = System.getProperty(VERTX_INFINISPAN_CONFIG_PROP_NAME, INFINISPAN_XML);
    this.jgroupsConfigPath = System.getProperty(VERTX_JGROUPS_CONFIG_PROP_NAME, JGROUPS_XML);
    userProvidedCacheManager = false;
  }

  /**
   * Creates a new cluster manager with an existing {@link DefaultCacheManager}.
   * It is your responsibility to start/stop the cache manager when the Vert.x instance joins/leaves the cluster.
   *
   * @param cacheManager the existing cache manager
   */
  public InfinispanClusterManager(DefaultCacheManager cacheManager) {
    Objects.requireNonNull(cacheManager, "cacheManager");
    this.cacheManager = cacheManager;
    ispnConfigPath = null;
    jgroupsConfigPath = null;
    userProvidedCacheManager = true;
  }

  @Override
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      @SuppressWarnings("unchecked")
      MultimapCacheManager<Object, Object> multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
      EmbeddedMultimapCache<Object, Object> multimapCache = (EmbeddedMultimapCache<Object, Object>) multimapCacheManager.get(name);
      InfinispanAsyncMultiMap<K, V> asyncMultiMap = new InfinispanAsyncMultiMap<>(vertx, multimapCache);
      synchronized (this) {
        multimaps.add(asyncMultiMap);
      }
      future.complete(asyncMultiMap);
    }, false, resultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> resultHandler) {
    vertx.executeBlocking(future -> {
      Cache<Object, Object> cache = cacheManager.getCache(name);
      future.complete(new InfinispanAsyncMapImpl<>(vertx, cache));
    }, false, resultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return cacheManager.getCache(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    ContextImpl context = (ContextImpl) vertx.getOrCreateContext();
    // Ordered on the internal blocking executor
    context.executeBlocking(() -> {
      java.util.concurrent.locks.Lock lock = lockService.getLock(name);
      try {
        if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
          return new JGroupsLock(vertx, lock);
        } else {
          throw new VertxException("Timed out waiting to get lock " + name);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VertxException(e);
      }
    }, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    vertx.executeBlocking(future -> {
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);
      if (!counterManager.isDefined(name)) {
        counterManager.defineCounter(name, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
      }
      future.complete(new InfinispanCounter(vertx, counterManager.getStrongCounter(name)));
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
      if (!userProvidedCacheManager) {
        InputStream ispnConfigStream = null;
        try {
          FileLookup fileLookup = FileLookupFactory.newInstance();

          ispnConfigStream = fileLookup.lookupFile(ispnConfigPath, getCTCCL());
          if (ispnConfigStream == null) {
            log.warn("Cannot find Infinispan config '" + ispnConfigPath + "', using default");
            ispnConfigStream = fileLookup.lookupFileStrict(DEFAULT_INFINISPAN_XML, getCTCCL());
          }
          ConfigurationBuilderHolder builderHolder = new ParserRegistry().parse(ispnConfigStream);
          // Workaround Launcher in fatjar issue (context classloader may be null)
          ClassLoader classLoader = getCTCCL();
          if (classLoader == null) {
            classLoader = getClass().getClassLoader();
          }
          builderHolder.getGlobalConfigurationBuilder().classLoader(classLoader);

          if (fileLookup.lookupFileLocation(jgroupsConfigPath, getCTCCL()) != null) {
            log.warn("Forcing JGroups config to '" + jgroupsConfigPath + "'");
            builderHolder.getGlobalConfigurationBuilder().transport().defaultTransport()
              .addProperty("configurationFile", jgroupsConfigPath);
          }

          cacheManager = new DefaultCacheManager(builderHolder, true);
        } catch (IOException e) {
          future.fail(e);
          return;
        } finally {
          safeClose(ispnConfigStream);
        }
      }
      viewListener = new ClusterViewListener();
      cacheManager.addListener(viewListener);
      JGroupsTransport transport = (JGroupsTransport) cacheManager.getTransport();
      JChannel channel = transport.getChannel();
      CENTRAL_LOCK centralLock = new CENTRAL_LOCK();
      centralLock.setValue("use_thread_id_for_lock_owner", Boolean.FALSE);
      centralLock.setBypassBundling(true);
      Protocol[] protocols = new Protocol[]{centralLock};
      Class<? extends Protocol> topProtocol = channel.getProtocolStack().getTopProtocol().getClass();
      try {
        forkChannel = new ForkChannel(channel, "vertx-infinispan-stack", "vertx-infinispan-channel", true, ABOVE, topProtocol, protocols);
        forkChannel.connect("ignored");
        lockService = new LockService(forkChannel);
        future.complete();
      } catch (Exception e) {
        future.fail(e);
      }
    }, false, resultHandler);
  }

  private ClassLoader getCTCCL() {
    return Thread.currentThread().getContextClassLoader();
  }

  private void safeClose(InputStream is) {
    if (is != null) {
      try {
        is.close();
      } catch (IOException ignored) {
      }
    }
  }

  @Override
  public void leave(Handler<AsyncResult<Void>> resultHandler) {
    vertx.executeBlocking(future -> {
      if (!active) {
        future.complete();
        return;
      }
      active = false;
      forkChannel.close();
      cacheManager.removeListener(viewListener);
      if (!userProvidedCacheManager) {
        cacheManager.stop();
      }
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
    public void handleViewChange(ViewChangedEvent e) {
      handleViewChangeInternal(e);
    }

    @Merged
    public void handleMerge(MergeEvent e) {
      handleViewChangeInternal(e);
    }

    private void handleViewChangeInternal(ViewChangedEvent e) {
      synchronized (InfinispanClusterManager.this) {
        if (!active) {
          return;
        }

        multimaps.forEach(InfinispanAsyncMultiMap::clearCache);

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
