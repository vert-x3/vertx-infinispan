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

import io.vertx.core.spi.cluster.RegistrationInfo;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

/**
 * @author Thomas Segismont
 */
@SerializeWith(InfinispanRegistrationInfo.InfinispanRegistrationInfoExternalizer.class)
public class InfinispanRegistrationInfo {

  private final RegistrationInfo registrationInfo;

  public InfinispanRegistrationInfo(RegistrationInfo registrationInfo) {
    this.registrationInfo = Objects.requireNonNull(registrationInfo);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InfinispanRegistrationInfo that = (InfinispanRegistrationInfo) o;

    return registrationInfo.equals(that.registrationInfo);
  }

  @Override
  public int hashCode() {
    return registrationInfo.hashCode();
  }

  public RegistrationInfo unwrap() {
    return registrationInfo;
  }

  public static class InfinispanRegistrationInfoExternalizer implements Externalizer<InfinispanRegistrationInfo> {
    @Override
    public void writeObject(ObjectOutput output, InfinispanRegistrationInfo object) throws IOException {
      output.writeUTF(object.registrationInfo.nodeId());
      output.writeLong(object.registrationInfo.seq());
      output.writeBoolean(object.registrationInfo.localOnly());
    }

    @Override
    public InfinispanRegistrationInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new InfinispanRegistrationInfo(new RegistrationInfo(input.readUTF(), input.readLong(), input.readBoolean()));
    }
  }
}
