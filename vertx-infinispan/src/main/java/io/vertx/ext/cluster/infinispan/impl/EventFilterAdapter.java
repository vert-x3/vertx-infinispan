package io.vertx.ext.cluster.infinispan.impl;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;

@ProtoAdapter(SubsCacheHelper.EventFilter.class) // Indicate the class you want to customize serialization for
public class EventFilterAdapter {

  @ProtoFactory
  SubsCacheHelper.EventFilter create() {
    return new SubsCacheHelper.EventFilter();
  }

}
