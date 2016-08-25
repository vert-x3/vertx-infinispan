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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Thomas Segismont
 */
@SerializeWith(InfinispanClusterSerializable.ClusterSerializableExternalizer.class)
public class InfinispanClusterSerializable {

  private final ClusterSerializable data;

  public InfinispanClusterSerializable(ClusterSerializable data) {
    Objects.requireNonNull(data);
    this.data = data;
  }

  public ClusterSerializable getData() {
    return data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InfinispanClusterSerializable that = (InfinispanClusterSerializable) o;
    return data.equals(that.data);
  }

  @Override
  public int hashCode() {
    return data.hashCode();
  }

  public static class ClusterSerializableExternalizer implements Externalizer<InfinispanClusterSerializable> {
    @Override
    public void writeObject(ObjectOutput output, InfinispanClusterSerializable object) throws IOException {
      output.writeUTF(object.data.getClass().getName());
      Buffer buffer = Buffer.buffer();
      object.data.writeToBuffer(buffer);
      byte[] bytes = buffer.getBytes();
      output.writeInt(bytes.length);
      output.write(bytes);
    }

    @Override
    public InfinispanClusterSerializable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      String className = input.readUTF();
      int length = input.readInt();
      byte[] bytes = new byte[length];
      input.readFully(bytes);
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      ClusterSerializable clusterSerializable;
      try {
        clusterSerializable = (ClusterSerializable) clazz.newInstance();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
      clusterSerializable.readFromBuffer(0, Buffer.buffer(bytes));
      return new InfinispanClusterSerializable(clusterSerializable);
    }
  }
}
