package io.vertx.ext.cluster.infinispan.impl;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;

@ProtoAdapter(SubsCacheHelper.EventConverter.class) // Indicate the class you want to customize serialization for
public class EventConverterAdapter {

  @ProtoFactory
  SubsCacheHelper.EventConverter create() {
    return new SubsCacheHelper.EventConverter();
  }

}
