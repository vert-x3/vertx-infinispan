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

import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.net.impl.ServerID;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Thomas Segismont
 */
@SerializeWith(InfinispanClusterNodeInfo.InfinispanClusterNodeInfoExternalizer.class)
public class InfinispanClusterNodeInfo {

  private final ClusterNodeInfo clusterNodeInfo;

  public InfinispanClusterNodeInfo(ClusterNodeInfo clusterNodeInfo) {
    Objects.requireNonNull(clusterNodeInfo);
    this.clusterNodeInfo = clusterNodeInfo;
  }

  public ClusterNodeInfo getClusterNodeInfo() {
    return clusterNodeInfo;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InfinispanClusterNodeInfo that = (InfinispanClusterNodeInfo) o;

    return clusterNodeInfo.equals(that.clusterNodeInfo);

  }

  @Override
  public int hashCode() {
    return clusterNodeInfo.hashCode();
  }

  public static class InfinispanClusterNodeInfoExternalizer implements Externalizer<InfinispanClusterNodeInfo> {
    @Override
    public void writeObject(ObjectOutput output, InfinispanClusterNodeInfo object) throws IOException {
      output.writeUTF(object.clusterNodeInfo.nodeId);
      output.writeInt(object.clusterNodeInfo.serverID.port);
      output.writeUTF(object.clusterNodeInfo.serverID.host);
    }

    @Override
    public InfinispanClusterNodeInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      ClusterNodeInfo clusterNodeInfo = new ClusterNodeInfo(input.readUTF(), new ServerID(input.readInt(), input.readUTF()));
      return new InfinispanClusterNodeInfo(clusterNodeInfo);
    }
  }
}
