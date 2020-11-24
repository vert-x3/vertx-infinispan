/*
 * Copyright 2020 Red Hat, Inc.
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

package io.vertx.ext.cluster.infinispan.impl;

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.multimap.impl.Bucket;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.util.function.SerializablePredicate;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.infinispan.notifications.Listener.Observation.POST;

/**
 * @author Thomas Segismont
 */
public class SubsCacheHelper {

  private static final Logger log = LoggerFactory.getLogger(SubsCacheHelper.class);

  private final EmbeddedMultimapCache<String, WrappedBytes> subsCache;
  private final NodeSelector nodeSelector;
  private final EntryListener entryListener;

  public SubsCacheHelper(DefaultCacheManager cacheManager, NodeSelector nodeSelector) {
    @SuppressWarnings("unchecked")
    MultimapCacheManager<String, WrappedBytes> multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
    subsCache = (EmbeddedMultimapCache<String, WrappedBytes>) multimapCacheManager.get("__vertx.subs");
    this.nodeSelector = nodeSelector;
    entryListener = new EntryListener();
    Set<Class<? extends Annotation>> filterAnnotations = Stream.<Class<? extends Annotation>>builder()
      .add(CacheEntryCreated.class)
      .add(CacheEntryModified.class)
      .add(CacheEntryRemoved.class)
      .build()
      .collect(toSet());
    subsCache.getCache()
      .addFilteredListener(entryListener, new EventFilter(), new EventConverter(), filterAnnotations);
  }

  public CompletableFuture<List<RegistrationInfo>> get(String address) {
    return subsCache.get(address)
      .thenApply(collection -> {
        return collection.stream()
          .map(WrappedBytes::getBytes)
          .map(DataConverter::<RegistrationInfo>fromCachedObject)
          .collect(toList());
      });
  }

  public CompletableFuture<Void> put(String address, RegistrationInfo registrationInfo) {
    byte[] bytes = DataConverter.toCachedObject(registrationInfo);
    return subsCache.put(address, new WrappedByteArray(bytes));
  }

  public CompletableFuture<Void> remove(String address, RegistrationInfo registrationInfo) {
    byte[] bytes = DataConverter.toCachedObject(registrationInfo);
    return subsCache.remove(address, new WrappedByteArray(bytes))
      .thenApply(v -> null);
  }

  public void removeAllForNode(String nodeId) {
    subsCache.remove((SerializablePredicate<WrappedBytes>) value -> {
      RegistrationInfo registrationInfo = DataConverter.fromCachedObject(value.getBytes());
      return nodeId.equals(registrationInfo.nodeId());
    });
  }

  public void close() {
    subsCache.getCache().removeListener(entryListener);
  }

  private void fireRegistrationUpdateEvent(String address) {
    get(address).whenComplete((registrationInfos, throwable) -> {
      if (throwable == null) {
        nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, registrationInfos));
      } else {
        log.trace("A failure occured while retrieving the updated registrations", throwable);
        nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, Collections.emptyList()));
      }
    });
  }

  @Listener(clustered = true, observation = POST, sync = false)
  private class EntryListener {

    public EntryListener() {
    }

    @CacheEntryCreated
    public void entryCreated(CacheEntryCreatedEvent<String, Void> event) {
      fireRegistrationUpdateEvent(event.getKey());
    }

    @CacheEntryModified
    public void entryModified(CacheEntryModifiedEvent<String, Void> event) {
      fireRegistrationUpdateEvent(event.getKey());
    }

    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent<String, Void> event) {
      fireRegistrationUpdateEvent(event.getKey());
    }
  }

  private static class EventFilter implements CacheEventFilter<String, Bucket<WrappedBytes>> {

    public EventFilter() {
    }

    @Override
    public boolean accept(String key, Bucket<WrappedBytes> oldValue, Metadata oldMetadata, Bucket<WrappedBytes> newValue, Metadata newMetadata, EventType eventType) {
      return true;
    }
  }

  private static class EventConverter implements CacheEventConverter<String, Bucket<WrappedBytes>, Void> {

    @Override
    public Void convert(String key, Bucket<WrappedBytes> oldValue, Metadata oldMetadata, Bucket<WrappedBytes> newValue, Metadata newMetadata, EventType eventType) {
      return null;
    }
  }
}
