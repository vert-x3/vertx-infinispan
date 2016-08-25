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

import org.jboss.logging.Logger;

import java.text.MessageFormat;

/**
 * @author Thomas Segismont
 */
public class JBossLoggerAdapter extends Logger {

  private final io.vertx.core.logging.Logger vertxLogger;

  public JBossLoggerAdapter(String name, io.vertx.core.logging.Logger vertxLogger) {
    super(name);
    this.vertxLogger = vertxLogger;
  }

  @Override
  protected void doLog(Level level, String loggerClassName, Object message, Object[] parameters, Throwable thrown) {
    if (!isEnabled(level)) {
      return;
    }
    try {
      String text;
      if (parameters == null || parameters.length == 0) {
        text = String.valueOf(message);
      } else {
        text = MessageFormat.format(String.valueOf(message), parameters);
      }
      logInternal(level, thrown, text);
    } catch (Throwable ignored) {
    }
  }

  private void logInternal(Level level, Throwable thrown, String text) {
    switch (level) {
      case FATAL:
        vertxLogger.fatal(text, thrown);
        break;
      case ERROR:
        vertxLogger.error(text, thrown);
        break;
      case WARN:
        vertxLogger.warn(text, thrown);
        break;
      case DEBUG:
        vertxLogger.debug(text, thrown);
        break;
      case TRACE:
        vertxLogger.trace(text, thrown);
        break;
      default:
        vertxLogger.info(text, thrown);
    }
  }

  @Override
  protected void doLogf(Level level, String loggerClassName, String format, Object[] parameters, Throwable thrown) {
    if (!isEnabled(level)) {
      return;
    }
    try {
      String text;
      if (parameters == null) {
        text = format;
      } else {
        text = String.format(format, parameters);
      }
      logInternal(level, thrown, text);
    } catch (Throwable ignored) {
    }
  }

  @Override
  public boolean isEnabled(Level level) {
    boolean enabled;
    switch (level) {
      case INFO:
        enabled = vertxLogger.isInfoEnabled();
        break;
      case DEBUG:
        enabled = vertxLogger.isDebugEnabled();
        break;
      case TRACE:
        enabled = vertxLogger.isTraceEnabled();
        break;
      default:
        enabled = true;
    }
    return enabled;
  }
}
