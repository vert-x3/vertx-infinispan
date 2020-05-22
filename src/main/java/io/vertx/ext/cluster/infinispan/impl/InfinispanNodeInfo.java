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
import io.vertx.core.shareddata.impl.ClusterSerializable;
import io.vertx.core.spi.cluster.NodeInfo;

import java.util.Objects;

/**
 * @author Thomas Segismont
 */
public class InfinispanNodeInfo implements ClusterSerializable {

  private NodeInfo nodeInfo;

  public InfinispanNodeInfo() {
  }

  public InfinispanNodeInfo(NodeInfo nodeInfo) {
    this.nodeInfo = Objects.requireNonNull(nodeInfo);
  }

  public NodeInfo unwrap() {
    return nodeInfo;
  }

  @Override
  public void writeToBuffer(Buffer buffer) {
    buffer.appendInt(nodeInfo.host().length()).appendString(nodeInfo.host());
    buffer.appendInt(nodeInfo.port());
    JsonObject metadata = nodeInfo.metadata();
    if (metadata != null) {
      Buffer metadataBuffer = metadata.toBuffer();
      buffer.appendInt(metadata.size()).appendBuffer(metadataBuffer);
    } else {
      buffer.appendInt(-1);
    }
  }

  @Override
  public int readFromBuffer(int pos, Buffer buffer) {
    int len = buffer.getInt(pos);
    pos += 4;
    String host = buffer.getString(pos, pos + len);
    pos += len;
    int port = buffer.getInt(pos);
    pos += 4;
    len = buffer.getInt(pos);
    pos += 4;
    JsonObject metadata;
    if (len < 0) {
      metadata = null;
    } else if (len == 0) {
      metadata = new JsonObject();
    } else {
      metadata = new JsonObject(buffer.getBuffer(pos, pos + len));
      pos += len;
    }
    nodeInfo = new NodeInfo(host, port, metadata);
    return pos;
  }
}
