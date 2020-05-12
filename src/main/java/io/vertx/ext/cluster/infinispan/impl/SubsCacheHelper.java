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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.infinispan.notifications.Listener.Observation.POST;

/**
 * @author Thomas Segismont
 */
public class SubsCacheHelper {

  private static final Logger log = LoggerFactory.getLogger(SubsCacheHelper.class);

  private final EmbeddedMultimapCache<String, InfinispanRegistrationInfo> subsCache;
  private final NodeSelector nodeSelector;
  private final EntryListener entryListener;

  public SubsCacheHelper(DefaultCacheManager cacheManager, NodeSelector nodeSelector) {
    @SuppressWarnings("unchecked")
    MultimapCacheManager<String, InfinispanRegistrationInfo> multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
    subsCache = (EmbeddedMultimapCache<String, InfinispanRegistrationInfo>) multimapCacheManager.get("__vertx.subs");
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

  public void get(String address, Promise<List<RegistrationInfo>> promise) {
    Future.fromCompletionStage(subsCache.get(address))
      .map(collection -> collection.stream().map(InfinispanRegistrationInfo::unwrap).collect(toList()))
      .onComplete(promise);
  }

  public void put(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    InfinispanRegistrationInfo value = new InfinispanRegistrationInfo(registrationInfo);
    Future.fromCompletionStage(subsCache.put(address, value)).onComplete(promise);
  }

  public void remove(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    InfinispanRegistrationInfo value = new InfinispanRegistrationInfo(registrationInfo);
    Future.fromCompletionStage(subsCache.remove(address, value)).<Void>mapEmpty().onComplete(promise);
  }

  public void removeAllForNode(String nodeId) {
    subsCache.remove((SerializablePredicate<InfinispanRegistrationInfo>) info -> nodeId.equals(info.unwrap().nodeId()));
  }

  public void close() {
    subsCache.getCache().removeListener(entryListener);
  }

  private void fireRegistrationUpdateEvent(String address) {
    Promise<List<RegistrationInfo>> promise = Promise.promise();
    get(address, promise);
    promise.future().onComplete(ar -> {
      if (ar.succeeded()) {
        nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, ar.result()));
      } else {
        log.trace("A failure occured while retrieving the updated registrations", ar.cause());
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

  @SerializeWith(EventFilterExternalizer.class)
  private static class EventFilter implements CacheEventFilter<String, Collection<InfinispanRegistrationInfo>> {

    public EventFilter() {
    }

    @Override
    public boolean accept(String key, Collection<InfinispanRegistrationInfo> oldValue, Metadata oldMetadata, Collection<InfinispanRegistrationInfo> newValue, Metadata newMetadata, EventType eventType) {
      return true;
    }
  }

  public static class EventFilterExternalizer implements Externalizer<EventFilter> {

    @Override
    public void writeObject(ObjectOutput objectOutput, EventFilter eventFilter) {
    }

    @Override
    public EventFilter readObject(ObjectInput objectInput) {
      return new EventFilter();
    }
  }

  @SerializeWith(EventConverterExternalizer.class)
  private static class EventConverter implements CacheEventConverter<String, Collection<InfinispanRegistrationInfo>, Void> {

    @Override
    public Void convert(String key, Collection<InfinispanRegistrationInfo> oldValue, Metadata oldMetadata, Collection<InfinispanRegistrationInfo> newValue, Metadata newMetadata, EventType eventType) {
      return null;
    }
  }

  public static class EventConverterExternalizer implements Externalizer<EventConverter> {

    @Override
    public void writeObject(ObjectOutput objectOutput, EventConverter eventConverter) {
    }

    @Override
    public EventConverter readObject(ObjectInput objectInput) {
      return new EventConverter();
    }
  }
}
