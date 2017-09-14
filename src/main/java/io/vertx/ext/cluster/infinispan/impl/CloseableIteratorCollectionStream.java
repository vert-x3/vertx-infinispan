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
import java.util.Deque;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Thomas Segismont
 */
public class CloseableIteratorCollectionStream<IN, OUT> implements ReadStream<OUT> {

  private static final int BATCH_SIZE = 10;

  private final Context context;
  private final Supplier<CloseableIteratorCollection<IN>> iterableSupplier;
  private final Function<IN, OUT> converter;

  private CloseableIteratorCollection<IN> iterable;
  private CloseableIterator<IN> iterator;
  private Deque<IN> queue;
  private Handler<OUT> dataHandler;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> endHandler;
  private boolean paused;
  private boolean readInProgress;
  private boolean closed;

  public CloseableIteratorCollectionStream(Context context, Supplier<CloseableIteratorCollection<IN>> iterableSupplier, Function<IN, OUT> converter) {
    this.context = context;
    this.iterableSupplier = iterableSupplier;
    this.converter = converter;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<IN, OUT> exceptionHandler(Handler<Throwable> handler) {
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
  public synchronized CloseableIteratorCollectionStream<IN, OUT> handler(Handler<OUT> handler) {
    checkClosed();
    if (handler == null) {
      close();
    } else {
      dataHandler = handler;
      context.<CloseableIteratorCollection<IN>>executeBlocking(fut -> fut.complete(iterableSupplier.get()), false, ar -> {
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
    return !paused && !closed;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<IN, OUT> pause() {
    checkClosed();
    paused = true;
    return this;
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<IN, OUT> resume() {
    checkClosed();
    if (paused) {
      paused = false;
      if (dataHandler != null) {
        doRead();
      }
    }
    return this;
  }

  private synchronized void doRead() {
    if (readInProgress) {
      return;
    }
    readInProgress = true;
    if (iterator == null) {
      context.<CloseableIterator<IN>>executeBlocking(fut -> fut.complete(iterable.iterator()), false, ar -> {
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
    for (int i = 0; i < BATCH_SIZE && iterator.hasNext(); i++) {
      queue.add(iterator.next());
    }
    if (queue.isEmpty()) {
      close();
      context.runOnContext(v -> {
        synchronized (this) {
          if (endHandler != null) {
            endHandler.handle(null);
          }
        }
      });
      return;
    }
    context.runOnContext(v -> emitQueued());
  }

  private void handleException(Throwable cause) {
    if (exceptionHandler != null) {
      exceptionHandler.handle(cause);
    }
  }

  private synchronized void emitQueued() {
    while (!queue.isEmpty() && canRead()) {
      dataHandler.handle(converter.apply(queue.remove()));
    }
    readInProgress = false;
    if (canRead()) {
      doRead();
    }
  }

  @Override
  public synchronized CloseableIteratorCollectionStream<IN, OUT> endHandler(Handler<Void> handler) {
    endHandler = handler;
    return this;
  }

  private void close() {
    closed = true;
    context.executeBlocking(fut -> {
      if (iterator != null) {
        iterator.close();
      }
      fut.complete();
    }, false, null);
  }
}
