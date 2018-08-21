/*
 * Copyright 2017 Red Hat, Inc.
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

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Thomas Segismont
 */
public class CloseableIteratorCollectionStream<I, O> implements ReadStream<O> {

  private static final int BATCH_SIZE = 10;

  private final Context context;
  private final Supplier<CloseableIteratorCollection<I>> iterableSupplier;
  private final Function<I, O> converter;

  private CloseableIteratorCollection<I> iterable;
  private CloseableIterator<I> iterator;
  private Deque<I> queue;
  private Handler<O> dataHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private long demand = Long.MAX_VALUE;
  private boolean readInProgress;
  private boolean closed;

  public CloseableIteratorCollectionStream(Context context, Supplier<CloseableIteratorCollection<I>> iterableSupplier, Function<I, O> converter) {
    this.context = context;
    this.iterableSupplier = iterableSupplier;
    this.converter = converter;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<I, O> exceptionHandler(Handler<Throwable> handler) {
    checkClosed();
    this.exceptionHandler = handler;
    return this;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalArgumentException("Stream is closed");
    }
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<I, O> handler(Handler<O> handler) {
    checkClosed();
    if (handler == null) {
      close();
    } else {
      dataHandler = handler;
      context.<CloseableIteratorCollection<I>>executeBlocking(fut -> fut.complete(iterableSupplier.get()), false, ar -> {
        synchronized (this) {
          if (ar.succeeded()) {
            iterable = ar.result();
            if (canRead()) {
              doRead();
            }
          } else {
            close();
            handleException(ar.cause());
          }
        }
      });
    }
    return this;
  }

  private boolean canRead() {
    return demand > 0L && !closed;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<I, O> pause() {
    checkClosed();
    demand = 0L;
    return this;
  }

  @Override
  public CloseableIteratorCollectionStream<I, O> fetch(long amount) {
    checkClosed();
    if (amount > 0L) {
      demand += amount;
      if (demand < 0L) {
        demand = Long.MAX_VALUE;
      }
      if (dataHandler != null) {
        doRead();
      }
    }
    return this;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<I, O> resume() {
    return fetch(Long.MAX_VALUE);
  }

  private synchronized void doRead() {
    if (readInProgress) {
      return;
    }
    readInProgress = true;
    if (iterator == null) {
      context.<CloseableIterator<I>>executeBlocking(fut -> fut.complete(iterable.iterator()), false, ar -> {
        synchronized (this) {
          readInProgress = false;
          if (ar.succeeded()) {
            iterator = ar.result();
            if (canRead()) {
              doRead();
            }
          } else {
            close();
            handleException(ar.cause());
          }
        }
      });
      return;
    }
    if (queue == null) {
      queue = new ArrayDeque<>(BATCH_SIZE);
    }
    if (!queue.isEmpty()) {
      context.runOnContext(v -> emitQueued());
      return;
    }
    context.<List<I>>executeBlocking(fut -> {
      List<I> batch = new ArrayList<>(BATCH_SIZE);
      for (int i = 0; i < BATCH_SIZE && iterator.hasNext(); i++) {
        batch.add(iterator.next());
      }
      fut.complete(batch);
    }, false, ar -> {
      synchronized (this) {
        if (ar.succeeded()) {
          queue.addAll(ar.result());
          if (queue.isEmpty()) {
            close();
            if (endHandler != null) {
              endHandler.handle(null);
            }
          } else {
            emitQueued();
          }
        } else {
          close();
          handleException(ar.cause());
        }
      }
    });
  }

  private void handleException(Throwable cause) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(cause);
    }
  }

  private synchronized void emitQueued() {
    while (!queue.isEmpty() && canRead()) {
      if (demand != Long.MAX_VALUE) {
        demand--;
      }
      dataHandler.handle(converter.apply(queue.remove()));
    }
    readInProgress = false;
    if (canRead()) {
      doRead();
    }
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<I, O> endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  private void close() {
    closed = true;
    AtomicReference<CloseableIterator<I>> iteratorRef = new AtomicReference<>();
    context.executeBlocking(fut -> {
      synchronized (this) {
        iteratorRef.set(iterator);
      }
      CloseableIterator<I> iter = iteratorRef.get();
      if (iter != null) {
        iter.close();
      }
      fut.complete();
    }, false, null);
  }
}
