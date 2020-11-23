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

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author Thomas Segismont
 */
public class DataConverter {

  public static byte[] toCachedObject(Object o) {
    if (o == null) {
      return null;
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    if (o instanceof Serializable) {
      baos.write(0);
      writeSerializable(baos, (Serializable) o);
    } else if (o instanceof ClusterSerializable) {
      baos.write(1);
      writeClusterSerializable(baos, (ClusterSerializable) o);
    } else {
      throw new IllegalArgumentException("Cannot convert object of type: " + o.getClass());
    }
    return baos.toByteArray();
  }

  private static void writeSerializable(ByteArrayOutputStream baos, Serializable o) {
    try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
      out.writeObject(o);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeClusterSerializable(ByteArrayOutputStream baos, ClusterSerializable o) {
    Buffer buffer = Buffer.buffer();
    byte[] classNameBytes = o.getClass().getName().getBytes(StandardCharsets.UTF_8);
    buffer.appendInt(classNameBytes.length).appendBytes(classNameBytes);
    o.writeToBuffer(buffer);
    baos.write(buffer.getBytes(), 0, buffer.length());
  }

  @SuppressWarnings("unchecked")
  public static <T> T fromCachedObject(byte[] value) {
    if (value == null) {
      return null;
    }
    byte type = value[0];
    if (type == 0) {
      return (T) readSerializable(value);
    } else if (type == 1) {
      return (T) readClusterSerializable(value);
    }
    throw new IllegalArgumentException("Cannot convert object of type: " + type);
  }

  private static Object readSerializable(byte[] value) {
    ByteArrayInputStream bais = new ByteArrayInputStream(value, 1, value.length - 1);
    try (ObjectInputStream in = new ObjectInputStream(bais)) {
      return in.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static ClusterSerializable readClusterSerializable(byte[] value) {
    Buffer buffer = Buffer.buffer(value);
    int pos = 1, len;
    len = buffer.getInt(pos);
    pos += 4;
    byte[] classNameBytes = buffer.getBytes(pos, pos + len);
    pos += len;
    ClusterSerializable clusterSerializable;
    try {
      Class<?> clazz = getClassLoader().loadClass(new String(classNameBytes, StandardCharsets.UTF_8));
      clusterSerializable = (ClusterSerializable) clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    clusterSerializable.readFromBuffer(pos, buffer);
    return clusterSerializable;
  }

  private static ClassLoader getClassLoader() {
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    return tccl != null ? tccl:DataConverter.class.getClassLoader();
  }

  private DataConverter() {
    // Utility class
  }
}
