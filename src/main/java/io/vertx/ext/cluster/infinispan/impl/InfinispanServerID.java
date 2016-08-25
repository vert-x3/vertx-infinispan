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
@SerializeWith(InfinispanServerID.InfinispanServerIDExternalizer.class)
public class InfinispanServerID {

  private final ServerID serverID;

  public InfinispanServerID(ServerID serverID) {
    Objects.requireNonNull(serverID);
    this.serverID = serverID;
  }

  public ServerID getServerID() {
    return serverID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InfinispanServerID that = (InfinispanServerID) o;

    return serverID.equals(that.serverID);

  }

  @Override
  public int hashCode() {
    return serverID.hashCode();
  }

  public static class InfinispanServerIDExternalizer implements Externalizer<InfinispanServerID> {
    @Override
    public void writeObject(ObjectOutput output, InfinispanServerID object) throws IOException {
      output.writeInt(object.serverID.port);
      output.writeUTF(object.serverID.host);
    }

    @Override
    public InfinispanServerID readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      ServerID serverID = new ServerID(input.readInt(), input.readUTF());
      return new InfinispanServerID(serverID);
    }
  }
}
