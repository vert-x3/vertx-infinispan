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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeAddress;
import io.vertx.core.spi.cluster.NodeInfo;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Thomas Segismont
 */
@SerializeWith(InfinispanNodeInfo.InfinispanNodeInfoExternalizer.class)
public class InfinispanNodeInfo {

  private final NodeInfo nodeInfo;

  public InfinispanNodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo = Objects.requireNonNull(nodeInfo);
  }

  public NodeInfo unwrap() {
    return nodeInfo;
  }

  public static class InfinispanNodeInfoExternalizer implements Externalizer<InfinispanNodeInfo> {
    @Override
    public void writeObject(ObjectOutput output, InfinispanNodeInfo object) throws IOException {
      output.writeUTF(object.nodeInfo.getAddress().getHost());
      output.writeInt(object.nodeInfo.getAddress().getPort());
      JsonObject metadata = object.nodeInfo.getMetadata();
      if (metadata != null) {
        byte[] bytes = metadata.toBuffer().getBytes();
        output.writeInt(bytes.length);
        output.write(bytes);
      } else {
        output.writeInt(-1);
      }
    }

    @Override
    public InfinispanNodeInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      NodeAddress address = new NodeAddress(input.readUTF(), input.readInt());
      int metadataBytesSize = input.readInt();
      JsonObject metadata;
      if (metadataBytesSize < 0) {
        metadata = null;
      } else {
        byte[] bytes = new byte[metadataBytesSize];
        input.readFully(bytes);
        metadata = new JsonObject(Buffer.buffer(bytes));
      }
      return new InfinispanNodeInfo(new NodeInfo(address, metadata));
    }
  }
}
