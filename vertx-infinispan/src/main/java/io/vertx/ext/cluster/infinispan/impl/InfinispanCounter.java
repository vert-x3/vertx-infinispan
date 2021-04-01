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

package io.vertx.ext.cluster.infinispan.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.Counter;
import org.infinispan.counter.api.SyncStrongCounter;

import java.util.Objects;

/**
 * @author Thomas Segismont
 */
public class InfinispanCounter implements Counter {

  private final Vertx vertx;
  private final SyncStrongCounter strongCounter;

  public InfinispanCounter(Vertx vertx, SyncStrongCounter strongCounter) {
    this.vertx = vertx;
    this.strongCounter = strongCounter;
  }

  @Override
  public Future<Long> get() {
    return vertx.executeBlocking(future -> {
      future.complete(strongCounter.getValue());
    }, false);
  }

  @Override
  public void get(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    get().onComplete(resultHandler);
  }

  @Override
  public Future<Long> incrementAndGet() {
    return addAndGet(1L);
  }

  @Override
  public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    addAndGet(1L, resultHandler);
  }

  @Override
  public Future<Long> getAndIncrement() {
    return getAndAdd(1L);
  }

  @Override
  public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    getAndAdd(1L, resultHandler);
  }

  @Override
  public Future<Long> decrementAndGet() {
    return addAndGet(-1L);
  }

  @Override
  public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    addAndGet(-1L, resultHandler);
  }

  @Override
  public Future<Long> addAndGet(long value) {
    return vertx.executeBlocking(future -> {
      future.complete(strongCounter.addAndGet(value));
    }, false);
  }

  @Override
  public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    addAndGet(value).onComplete(resultHandler);
  }

  @Override
  public Future<Long> getAndAdd(long value) {
    return vertx.executeBlocking(future -> {
      future.complete(strongCounter.addAndGet(value) - value);
    }, false);
  }

  @Override
  public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    getAndAdd(value).onComplete(resultHandler);
  }

  @Override
  public Future<Boolean> compareAndSet(long expected, long value) {
    return vertx.executeBlocking(future -> {
      future.complete(strongCounter.compareAndSet(expected, value));
    }, false);
  }

  @Override
  public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
    Objects.requireNonNull(resultHandler, "resultHandler");
    compareAndSet(expected, value).onComplete(resultHandler);
  }
}
