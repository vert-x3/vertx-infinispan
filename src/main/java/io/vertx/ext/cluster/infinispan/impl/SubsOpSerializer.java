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

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.spi.cluster.RegistrationInfo;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * @author Thomas Segismont
 */
public class SubsOpSerializer implements BiConsumer<Object, Throwable> {

  private final ContextInternal context;
  private final Queue<Task> tasks;

  private SubsOpSerializer(ContextInternal context) {
    this.context = context;
    tasks = new LinkedList<>();
  }

  public static SubsOpSerializer get(ContextInternal context) {
    ConcurrentMap<Object, Object> contextData = context.contextData();
    SubsOpSerializer instance = (SubsOpSerializer) contextData.get(SubsOpSerializer.class);
    if (instance == null) {
      SubsOpSerializer candidate = new SubsOpSerializer(context);
      SubsOpSerializer previous = (SubsOpSerializer) contextData.putIfAbsent(SubsOpSerializer.class, candidate);
      instance = previous == null ? candidate : previous;
    }
    return instance;
  }

  public void execute(BiFunction<String, RegistrationInfo, CompletableFuture<Void>> op, String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    if (Vertx.currentContext() != context) {
      context.runOnContext(v -> execute(op, address, registrationInfo, promise));
      return;
    }
    tasks.add(new Task(op, address, registrationInfo, promise));
    if (tasks.size() == 1) {
      processTask(tasks.peek());
    }
  }

  private void processTask(Task task) {
    if (Vertx.currentContext() != context) {
      throw new IllegalStateException();
    }
    CompletableFuture<Void> future = task.op.apply(task.address, task.registrationInfo);
    future.whenCompleteAsync(this, context);
  }

  @Override
  public void accept(Object o, Throwable throwable) {
    if (Vertx.currentContext() != context) {
      throw new IllegalStateException();
    }
    Task task = tasks.remove();
    if (throwable == null) {
      task.promise.complete();
    } else {
      task.promise.fail(throwable);
    }
    Task next = tasks.peek();
    if (next != null) {
      processTask(next);
    }
  }

  private static class Task {
    final BiFunction<String, RegistrationInfo, CompletableFuture<Void>> op;
    final String address;
    final RegistrationInfo registrationInfo;
    final Promise<Void> promise;

    Task(BiFunction<String, RegistrationInfo, CompletableFuture<Void>> op, String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
      this.op = op;
      this.address = address;
      this.registrationInfo = registrationInfo;
      this.promise = promise;
    }
  }
}
