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

package io.vertx.ext.cluster.infinispan.test;

import io.vertx.core.logging.Logger;
import org.jgroups.logging.Log;

import java.util.function.Consumer;

/**
 * @author Thomas Segismont
 */
public class JGroupsLoggerAdapter implements Log {

  private final Logger logger;

  public JGroupsLoggerAdapter(Logger logger) {
    this.logger = logger;
  }

  @Override
  public boolean isFatalEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public void fatal(String msg) {
    logger.fatal(msg);
  }

  @Override
  public void fatal(String msg, Object... args) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    withComputedText(msg, args, logger::fatal);
  }

  private void withComputedText(String msg, Object[] args, Consumer<String> consumer) {
    try {
      String text;
      if (args == null || args.length == 0) {
        text = msg;
      } else {
        text = String.format(msg, args);
      }
      consumer.accept(text);
    } catch (Throwable ignored) {
    }
  }

  @Override
  public void fatal(String msg, Throwable throwable) {
    logger.fatal(msg, throwable);
  }

  @Override
  public void error(String msg) {
    logger.error(msg);
  }

  @Override
  public void error(String format, Object... args) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    withComputedText(format, args, logger::error);
  }

  @Override
  public void error(String msg, Throwable throwable) {
    logger.error(msg, throwable);
  }

  @Override
  public void warn(String msg) {
    logger.warn(msg);
  }

  @Override
  public void warn(String msg, Object... args) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    withComputedText(msg, args, logger::warn);
  }

  @Override
  public void warn(String msg, Throwable throwable) {
    logger.warn(msg, throwable);
  }

  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  @Override
  public void info(String msg, Object... args) {
    if (!logger.isInfoEnabled()) {
      return;
    }
    withComputedText(msg, args, logger::info);
  }

  @Override
  public void debug(String msg) {
    logger.debug(msg);
  }

  @Override
  public void debug(String msg, Object... args) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    withComputedText(msg, args, logger::debug);
  }

  @Override
  public void debug(String msg, Throwable throwable) {
    logger.debug(msg, throwable);
  }

  @Override
  public void trace(Object msg) {
    logger.trace(String.valueOf(msg));
  }

  @Override
  public void trace(String msg) {
    logger.trace(msg);
  }

  @Override
  public void trace(String msg, Object... args) {
    if (!logger.isTraceEnabled()) {
      return;
    }
    withComputedText(msg, args, logger::trace);
  }

  @Override
  public void trace(String msg, Throwable throwable) {
    logger.trace(msg, throwable);
  }

  @Override
  public void setLevel(String level) {
  }

  @Override
  public String getLevel() {
    if (isTraceEnabled()) return "trace";
    if (isDebugEnabled()) return "debug";
    if (isInfoEnabled()) return "info";
    return "warn";
  }
}
