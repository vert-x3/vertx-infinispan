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

import io.vertx.core.Handler;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationListener;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.metadata.Metadata;
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.infinispan.notifications.Listener.Observation.POST;

/**
 * @author Thomas Segismont
 */
public class InfinispanRegistrationListener implements RegistrationListener {

  private Handler<Throwable> exceptionHandler;

  private interface InternalState {
    List<RegistrationInfo> initialState();

    void start();

    void stop();
  }

  private final VertxInternal vertx;
  private final EmbeddedMultimapCache<String, InfinispanRegistrationInfo> subsCache;
  private final String address;
  private final AtomicReference<InternalState> internalState;

  private Handler<List<RegistrationInfo>> handler;
  private Handler<Void> endHandler;

  public InfinispanRegistrationListener(VertxInternal vertx, EmbeddedMultimapCache<String, InfinispanRegistrationInfo> subsCache, String address, List<RegistrationInfo> infos) {
    this.vertx = vertx;
    this.subsCache = subsCache;
    this.address = address;
    internalState = new AtomicReference<>(new IdleState(infos));
  }

  @Override
  public List<RegistrationInfo> initialState() {
    return internalState.get().initialState();
  }

  @Override
  public synchronized RegistrationListener handler(Handler<List<RegistrationInfo>> handler) {
    this.handler = handler;
    return this;
  }

  private synchronized Handler<List<RegistrationInfo>> getHandler() {
    return handler;
  }

  @Override
  public RegistrationListener exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  private synchronized Handler<Throwable> getExceptionHandler() {
    return exceptionHandler;
  }

  @Override
  public synchronized RegistrationListener endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  private synchronized Handler<Void> getEndHandler() {
    return endHandler;
  }

  @Override
  public void start() {
    internalState.get().start();
  }

  @Override
  public void stop() {
    internalState.get().stop();
  }

  private class IdleState implements InternalState {

    final List<RegistrationInfo> infos;

    IdleState(List<RegistrationInfo> infos) {
      this.infos = infos;
    }

    @Override
    public List<RegistrationInfo> initialState() {
      return infos;
    }

    @Override
    public void start() {
      StartedState startedState = new StartedState();
      if (internalState.compareAndSet(this, startedState)) {
        startedState.init(infos);
      }
    }

    @Override
    public void stop() {
      internalState.compareAndSet(this, new StoppedState());
    }
  }

  private class StartedState implements InternalState {

    final TaskQueue taskQueue = new TaskQueue();

    EntryListener entryListener;
    List<RegistrationInfo> initial, last;

    void init(List<RegistrationInfo> infos) {
      taskQueue.execute(() -> {
        if (this != internalState.get()) {
          return;
        }
        initial = infos;
        Set<Class<? extends Annotation>> filterAnnotations = Stream.<Class<? extends Annotation>>builder()
          .add(CacheEntryCreated.class)
          .add(CacheEntryModified.class)
          .add(CacheEntryRemoved.class)
          .build()
          .collect(toSet());
        entryListener = new EntryListener(this::subsChanged);
        subsCache.getCache()
          .addFilteredListener(entryListener, new EventFilter(address), new EventConverter(), filterAnnotations);
        subsChanged(); // make sure state is checked if entry is removed before listener is registered
      }, vertx.getWorkerPool());
    }

    void subsChanged() {
      taskQueue.execute(() -> {
        Throwable caught;
        try {
          Collection<InfinispanRegistrationInfo> infos = subsCache.get(address).get();
          handleDataUpdate(infos.stream().map(InfinispanRegistrationInfo::unwrap).collect(toList()));
          caught = null;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          stop();
          caught = e;
        } catch (ExecutionException e) {
          stop();
          caught = e.getCause();
        }
        Handler<Throwable> eh;
        if (caught != null && (eh = getExceptionHandler()) != null) {
          eh.handle(caught);
        }
      }, vertx.getWorkerPool());
    }

    void handleDataUpdate(List<RegistrationInfo> infos) {
      if (this != internalState.get()) {
        return;
      }
      Runnable emission;
      if (initial != null) {
        if (infos.isEmpty()) {
          emission = terminalEvent();
        } else if (!initial.equals(infos)) {
          emission = itemEvent(infos);
        } else {
          emission = null;
        }
        last = infos;
        initial = null;
      } else if (last.isEmpty() || last.equals(infos)) {
        emission = null;
      } else {
        last = infos;
        if (last.isEmpty()) {
          emission = terminalEvent();
        } else {
          emission = itemEvent(infos);
        }
      }
      if (emission != null) {
        emission.run();
      }
    }

    private Runnable itemEvent(List<RegistrationInfo> infos) {
      Handler<List<RegistrationInfo>> h = getHandler();
      return () -> {
        if (h != null) {
          h.handle(infos);
        }
      };
    }

    private synchronized Runnable terminalEvent() {
      Handler<Void> e = getEndHandler();
      return () -> {
        stop();
        if (e != null) {
          e.handle(null);
        }
      };
    }

    @Override
    public List<RegistrationInfo> initialState() {
      return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
      if (internalState.compareAndSet(this, new StoppedState())) {
        taskQueue.execute(() -> {
          if (entryListener != null) {
            subsCache.getCache().removeListener(entryListener);
          }
        }, vertx.getWorkerPool());
      }
    }
  }

  private class StoppedState implements InternalState {

    @Override
    public List<RegistrationInfo> initialState() {
      return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }
  }


  @Listener(clustered = true, observation = POST, sync = false)
  private class EntryListener {

    private final Runnable callback;

    public EntryListener(Runnable callback) {
      this.callback = callback;
    }

    @CacheEntryCreated
    public void entryCreated(CacheEntryCreatedEvent<String, Void> event) {
      callback.run();
    }

    @CacheEntryModified
    public void entryModified(CacheEntryModifiedEvent<String, Void> event) {
      callback.run();
    }

    @CacheEntryRemoved
    public void entryRemoved(CacheEntryRemovedEvent<String, Void> event) {
      callback.run();
    }
  }

  @SerializeWith(EventFilterExternalizer.class)
  private static class EventFilter implements CacheEventFilter<String, Collection<InfinispanRegistrationInfo>> {

    private final String address;

    public EventFilter(String address) {
      this.address = address;
    }

    @Override
    public boolean accept(String key, Collection<InfinispanRegistrationInfo> oldValue, Metadata oldMetadata, Collection<InfinispanRegistrationInfo> newValue, Metadata newMetadata, EventType eventType) {
      return address.equals(key);
    }
  }

  public static class EventFilterExternalizer implements Externalizer<EventFilter> {

    @Override
    public void writeObject(ObjectOutput objectOutput, EventFilter eventFilter) throws IOException {
      objectOutput.writeUTF(eventFilter.address);
    }

    @Override
    public EventFilter readObject(ObjectInput objectInput) throws IOException {
      return new EventFilter(objectInput.readUTF());
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
