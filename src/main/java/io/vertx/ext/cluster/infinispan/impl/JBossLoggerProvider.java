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

import io.vertx.core.logging.LoggerFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.LoggerProvider;

import java.util.Map;

/**
 * @author Thomas Segismont
 */
public class JBossLoggerProvider implements LoggerProvider {

  @Override
  public Logger getLogger(String name) {
    return new JBossLoggerAdapter(name, LoggerFactory.getLogger(name));
  }

  @Override
  public void clearMdc() {
  }

  @Override
  public Object putMdc(String key, Object value) {
    return null;
  }

  @Override
  public Object getMdc(String key) {
    return null;
  }

  @Override
  public void removeMdc(String key) {
  }

  @Override
  public Map<String, Object> getMdcMap() {
    return null;
  }

  @Override
  public void clearNdc() {
  }

  @Override
  public String getNdc() {
    return null;
  }

  @Override
  public int getNdcDepth() {
    return 0;
  }

  @Override
  public String popNdc() {
    return null;
  }

  @Override
  public String peekNdc() {
    return null;
  }

  @Override
  public void pushNdc(String message) {
  }

  @Override
  public void setNdcMaxDepth(int maxDepth) {
  }
}
