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

import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.shareddata.impl.ClusterSerializable;

/**
 * @author Thomas Segismont
 */
public class DataConverter {

  public static <T> Object toCachedObject(T t) {
    if (t instanceof ServerID) {
      return new InfinispanServerID((ServerID) t);
    }
    if (t instanceof ClusterNodeInfo) {
      return new InfinispanClusterNodeInfo((ClusterNodeInfo) t);
    }
    if (t instanceof ClusterSerializable) {
      return new InfinispanClusterSerializable((ClusterSerializable) t);
    }
    return t;
  }

  @SuppressWarnings("unchecked")
  public static <T> T fromCachedObject(Object value) {
    if (value instanceof InfinispanServerID) {
      return (T) ((InfinispanServerID) value).getServerID();
    }
    if (value instanceof InfinispanClusterNodeInfo) {
      return (T) ((InfinispanClusterNodeInfo) value).getClusterNodeInfo();
    }
    if (value instanceof InfinispanClusterSerializable) {
      return (T) ((InfinispanClusterSerializable) value).getData();
    }
    return (T) value;
  }

  private DataConverter() {
    // Utility class
  }
}
