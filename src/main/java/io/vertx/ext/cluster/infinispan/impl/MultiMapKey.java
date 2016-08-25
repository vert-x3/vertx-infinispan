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

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Thomas Segismont
 */
@SerializeWith(MultiMapKey.MultiMapKeyExternalizer.class)
public class MultiMapKey<K, V> {
  private final K key;
  private final V value;


  public MultiMapKey(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MultiMapKey<?, ?> that = (MultiMapKey<?, ?>) o;
    return key.equals(that.key) && value.equals(that.value);

  }

  @Override
  public int hashCode() {
    int result = key.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }

  public static class MultiMapKeyExternalizer implements Externalizer<MultiMapKey> {
    @Override
    public void writeObject(ObjectOutput output, MultiMapKey object) throws IOException {
      output.writeObject(object.key);
      output.writeObject(object.value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiMapKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return new MultiMapKey(input.readObject(), input.readObject());
    }
  }
}
