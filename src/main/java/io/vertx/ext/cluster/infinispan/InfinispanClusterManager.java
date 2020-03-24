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

import io.vertx.core.Future;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import io.vertx.ext.cluster.infinispan.impl.*;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.context.Flag;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.impl.manager.EmbeddedClusteredLockManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.multimap.impl.EmbeddedMultimapCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.function.SerializablePredicate;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

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

  private VertxInternal vertx;
  private DefaultCacheManager cacheManager;
  private NodeListener nodeListener;
  private EmbeddedMultimapCacheManager<String, InfinispanRegistrationInfo> multimapCacheManager;
  private EmbeddedClusteredLockManager lockManager;
  private CounterManager counterManager;
  private NodeInfo nodeInfo;
  private AdvancedCache<String, InfinispanNodeInfo> nodeInfoCache;
  private EmbeddedMultimapCache<String, InfinispanRegistrationInfo> subsCache;
  private volatile boolean active;
  private ClusterViewListener viewListener;

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
  public void setVertx(VertxInternal vertx) {
    this.vertx = vertx;
  }

  public BasicCacheContainer getCacheContainer() {
    return cacheManager;
  }

  @Override
  public <K, V> Future<AsyncMap<K, V>> getAsyncMap(String name) {
    return vertx.executeBlocking(future -> {
      EmbeddedCacheManagerAdmin administration = cacheManager.administration();
      Cache<Object, Object> cache = administration.getOrCreateCache(name, "__vertx.distributed.cache.configuration");
      future.complete(new InfinispanAsyncMapImpl<>(vertx, cache));
    }, false);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return cacheManager.getCache(name);
  }

  @Override
  public Future<Lock> getLockWithTimeout(String name, long timeout) {
    Future<ClusteredLock> lockFuture = vertx.executeBlocking(promise -> {
      if (!lockManager.isDefined(name)) {
        lockManager.defineLock(name);
      }
      promise.complete(lockManager.get(name));
    }, false);
    return lockFuture.compose(lock -> {
      ContextInternal context = vertx.getOrCreateContext();
      return Future.fromCompletionStage(lock.tryLock(timeout, TimeUnit.MILLISECONDS), context)
        .compose(locked -> locked ? Future.succeededFuture(new InfinispanLock(lock)) : context.failedFuture("Timed out waiting to get lock " + name));
    });
  }

  @Override
  public Future<Counter> getCounter(String name) {
    return vertx.executeBlocking(future -> {
      if (!counterManager.isDefined(name)) {
        counterManager.defineCounter(name, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
      }
      future.complete(new InfinispanCounter(vertx, counterManager.getStrongCounter(name).sync()));
    }, false);
  }

  @Override
  public String getNodeId() {
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
  public Future<Void> setNodeInfo(NodeInfo nodeInfo) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    InfinispanNodeInfo value = new InfinispanNodeInfo(nodeInfo);
    CompletableFuture<InfinispanNodeInfo> completionStage = nodeInfoCache.withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(getNodeId(), value);
    return Future.fromCompletionStage(completionStage, vertx.getOrCreateContext()).mapEmpty();
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public Future<NodeInfo> getNodeInfo(String nodeId) {
    return Future.fromCompletionStage(nodeInfoCache.getAsync(nodeId), vertx.getOrCreateContext())
      .map(value -> value != null ? value.unwrap() : null);
  }

  @Override
  public Future<Void> join() {
    return vertx.executeBlocking(future -> {
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
      try {

        multimapCacheManager = (EmbeddedMultimapCacheManager<String, InfinispanRegistrationInfo>) EmbeddedMultimapCacheManagerFactory.from(cacheManager);
        subsCache = (EmbeddedMultimapCache<String, InfinispanRegistrationInfo>) multimapCacheManager.get("__vertx.subs");

        nodeInfoCache = cacheManager.<String, InfinispanNodeInfo>getCache("__vertx.nodeInfo").getAdvancedCache();

        lockManager = (EmbeddedClusteredLockManager) EmbeddedClusteredLockManagerFactory.from(cacheManager);
        counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);

        future.complete();
      } catch (Exception e) {
        future.fail(e);
      }
    }, false);
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
  public Future<Void> leave() {
    return vertx.executeBlocking(future -> {
      if (!active) {
        future.complete();
        return;
      }
      active = false;
      cacheManager.removeListener(viewListener);
      if (!userProvidedCacheManager) {
        cacheManager.stop();
      }
      future.complete();
    }, false);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public Future<Void> register(String address, RegistrationInfo registrationInfo) {
    InfinispanRegistrationInfo value = new InfinispanRegistrationInfo(registrationInfo);
    return Future.fromCompletionStage(subsCache.put(address, value), vertx.getOrCreateContext());
  }

  @Override
  public Future<Void> unregister(String address, RegistrationInfo registrationInfo) {
    InfinispanRegistrationInfo value = new InfinispanRegistrationInfo(registrationInfo);
    return Future.fromCompletionStage(subsCache.remove(address, value), vertx.getOrCreateContext()).mapEmpty();

  }

  @Override
  public Future<RegistrationListener> registrationListener(String address) {
    return Future.fromCompletionStage(subsCache.get(address), vertx.getOrCreateContext())
      .map(infos -> infos.stream().map(InfinispanRegistrationInfo::unwrap).collect(toList()))
      .map(infos -> new InfinispanRegistrationListener(vertx, subsCache, address, infos));
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

        List<Address> added = new ArrayList<>(e.getNewMembers());
        added.removeAll(e.getOldMembers());
        if (log.isDebugEnabled()) {
          log.debug("Members added = " + added);
        }
        added.forEach(address -> {
          if (nodeListener != null) {
            nodeListener.nodeAdded(address.toString());
          }
        });

        List<Address> removed = new ArrayList<>(e.getOldMembers());
        removed.removeAll(e.getNewMembers());
        if (log.isDebugEnabled()) {
          log.debug("Members removed = " + removed);
        }
        if (isMaster()) {
          cleanSubs(removed);
          cleanNodeInfos(removed);
        }
        removed.forEach(address -> {
          if (nodeListener != null) {
            nodeListener.nodeLeft(address.toString());
          }
        });
      }
    }
  }

  private boolean isMaster() {
    return cacheManager.isCoordinator();
  }

  private void cleanSubs(List<Address> removed) {
    removed.stream().map(Address::toString).forEach(nid -> {
      subsCache.remove((SerializablePredicate<InfinispanRegistrationInfo>) info -> nid.equals(info.unwrap().getNodeId()));
    });
  }

  private void cleanNodeInfos(List<Address> removed) {
    removed.stream().map(Address::toString).forEach(nodeInfoCache::remove);
  }
}
