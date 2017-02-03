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

package io.vertx.ext.cluster.infinispan.impl;

import io.vertx.core.Context;
import org.infinispan.commons.util.concurrent.FutureListener;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Thomas Segismont
 */
public class FutureAdapter<T> implements FutureListener<T> {

  private final Context context;
  private final io.vertx.core.Future<T> vertxFuture;

  public FutureAdapter(Context context) {
    this.context = context;
    this.vertxFuture = io.vertx.core.Future.future();
  }

  public io.vertx.core.Future<T> getVertxFuture() {
    return vertxFuture;
  }

  @Override
  public void futureDone(Future<T> future) {
    try {
      T result = future.get();
      context.runOnContext(v -> vertxFuture.complete(result));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      context.runOnContext(v -> vertxFuture.fail(e));
    } catch (ExecutionException e) {
      context.runOnContext(v -> vertxFuture.fail(e.getCause()));
    } catch (CancellationException e) {
      context.runOnContext(v -> vertxFuture.fail(e));
    }
  }
}
