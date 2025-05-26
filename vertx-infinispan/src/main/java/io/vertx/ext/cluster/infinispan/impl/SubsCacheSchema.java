package io.vertx.ext.cluster.infinispan.impl;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;

@AutoProtoSchemaBuilder(
    includeClasses = {
        EventFilterAdapter.class,
        EventConverterAdapter.class,
    },
    schemaFileName = "subs_cache.proto",
    schemaFilePath = "proto/")
public interface SubsCacheSchema extends SerializationContextInitializer {
}
