package io.vertx.ext.cluster.infinispan.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
    includeClasses = {
        EventFilterAdapter.class,
        EventConverterAdapter.class,
    },
    schemaFileName = "subs_cache.proto",
    schemaFilePath = "proto/",
    schemaPackageName = "subcache")
public interface SubsCacheSchema extends SerializationContextInitializer {
}
