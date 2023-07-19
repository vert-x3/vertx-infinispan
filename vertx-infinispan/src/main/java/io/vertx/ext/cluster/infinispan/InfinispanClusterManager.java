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

package io.vertx.ext.cluster.infinispan;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.future.PromiseInternal;
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
import org.infinispan.commons.api.CacheContainerAdmin;
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
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.stack.Protocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private NodeSelector nodeSelector;
  private DefaultCacheManager cacheManager;
  private NodeListener nodeListener;
  private EmbeddedClusteredLockManager lockManager;
  private CounterManager counterManager;
  private NodeInfo nodeInfo;
  private AdvancedCache<String, byte[]> nodeInfoCache;
  private SubsCacheHelper subsCacheHelper;
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
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  public BasicCacheContainer getCacheContainer() {
    return cacheManager;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    vertx.<AsyncMap<K, V>>executeBlocking(() -> {
      EmbeddedCacheManagerAdmin administration = cacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE);
      Cache<byte[], byte[]> cache = administration.getOrCreateCache(name, "__vertx.distributed.cache.configuration");
      return new InfinispanAsyncMapImpl<>(vertx, cache);
    }, false).onComplete(promise);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return cacheManager.getCache(name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> prom) {
    vertx.executeBlocking(() -> {
      PromiseInternal<Lock> promise = vertx.promise();
      if (!lockManager.isDefined(name)) {
        lockManager.defineLock(name);
      }
      ClusteredLock lock = lockManager.get(name);
      lock.tryLock(timeout, TimeUnit.MILLISECONDS).whenComplete((locked, throwable) -> {
        if (throwable == null) {
          if (locked) {
            promise.complete(new InfinispanLock(lock));
          } else {
            promise.fail("Timed out waiting to get lock " + name);
          }
        } else {
          promise.fail(throwable);
        }
      });
      return promise.future();
    }, false).compose(f -> f).onComplete(prom);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    vertx.<Counter>executeBlocking(() -> {
      if (!counterManager.isDefined(name)) {
        counterManager.defineCounter(name, CounterConfiguration.builder(CounterType.UNBOUNDED_STRONG).build());
      }
      return new InfinispanCounter(vertx, counterManager.getStrongCounter(name).sync());
    }, false).onComplete(promise);
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
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    byte[] value = DataConverter.toCachedObject(nodeInfo);
    Future.fromCompletionStage(nodeInfoCache.withFlags(Flag.IGNORE_RETURN_VALUES).putAsync(getNodeId(), value))
      .<Void>mapEmpty()
      .onComplete(promise);
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    nodeInfoCache.getAsync(nodeId).whenComplete((nodeInfo, throwable) -> {
      if (throwable != null) {
        promise.fail(throwable);
      } else if (nodeInfo == null) {
        promise.fail("Not a member of the cluster");
      } else {
        promise.complete(DataConverter.fromCachedObject(nodeInfo));
      }
    });
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      if (active) {
        return null;
      }
      active = true;
      if (!userProvidedCacheManager) {
        FileLookup fileLookup = FileLookupFactory.newInstance();

        URL ispnConfig = fileLookup.lookupFileLocation(ispnConfigPath, getCTCCL());
        if (ispnConfig == null) {
          log.warn("Cannot find Infinispan config '" + ispnConfigPath + "', using default");
          ispnConfig = fileLookup.lookupFileLocation(DEFAULT_INFINISPAN_XML, getCTCCL());
        }
        ConfigurationBuilderHolder builderHolder = new ParserRegistry().parse(ispnConfig);
        // Workaround Launcher in fatjar issue (context classloader may be null)
        ClassLoader classLoader = getCTCCL();
        if (classLoader == null) {
          classLoader = getClass().getClassLoader();
        }
        builderHolder.getGlobalConfigurationBuilder().classLoader(classLoader);

        if (fileLookup.lookupFileLocation(jgroupsConfigPath, getCTCCL()) != null) {
          log.warn("Forcing JGroups config to '" + jgroupsConfigPath + "'");
          builderHolder.getGlobalConfigurationBuilder().transport().defaultTransport()
            .removeProperty(JGroupsTransport.CHANNEL_CONFIGURATOR)
            .addProperty(JGroupsTransport.CONFIGURATION_FILE, jgroupsConfigPath);
        }

        cacheManager = new DefaultCacheManager(builderHolder, true);
      }
      viewListener = new ClusterViewListener();
      cacheManager.addListener(viewListener);

      subsCacheHelper = new SubsCacheHelper(vertx, cacheManager, nodeSelector);

      nodeInfoCache = cacheManager.<String, byte[]>getCache("__vertx.nodeInfo").getAdvancedCache();

      lockManager = (EmbeddedClusteredLockManager) EmbeddedClusteredLockManagerFactory.from(cacheManager);
      counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);

      return null;
    }, false).onComplete(promise);
  }

  private ClassLoader getCTCCL() {
    return Thread.currentThread().getContextClassLoader();
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      if (!active) {
        return null;
      }
      active = false;
      subsCacheHelper.close();
      cacheManager.removeListener(viewListener);
      if (!userProvidedCacheManager) {
        cacheManager.stop();
      }
      return null;
    }, false).onComplete(promise);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void addRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    SubsOpSerializer serializer = SubsOpSerializer.get(vertx.getOrCreateContext());
    serializer.execute(subsCacheHelper::put, address, registrationInfo, promise);
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    SubsOpSerializer serializer = SubsOpSerializer.get(vertx.getOrCreateContext());
    serializer.execute(subsCacheHelper::remove, address, registrationInfo, promise);
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    Future.fromCompletionStage(subsCacheHelper.get(address)).onComplete(promise);
  }

  @Override
  public String clusterHost() {
    return getHostFromTransportProtocol("bind_addr");
  }

  @Override
  public String clusterPublicHost() {
    return getHostFromTransportProtocol("external_addr");
  }

  private String getHostFromTransportProtocol(String fieldName) {
    JGroupsTransport transport = (JGroupsTransport) cacheManager.getTransport();
    Protocol bottomProtocol = transport.getChannel().getProtocolStack().getBottomProtocol();
    try {
      InetAddress external_addr = (InetAddress) bottomProtocol.getValue(fieldName);
      String str = external_addr.toString();
      if (str.charAt(0) == '/') {
        return str.substring(1);
      }
      return str.substring(0, str.indexOf('/'));
    } catch (Exception ignored) {
      return null;
    }
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
    removed.stream().map(Address::toString).forEach(subsCacheHelper::removeAllForNode);
  }

  private void cleanNodeInfos(List<Address> removed) {
    removed.stream().map(Address::toString).forEach(nodeInfoCache::remove);
  }
}
