package io.vertx.ext.cluster.infinispan.impl;

import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.types.java.util.UUIDAdapter;

import java.awt.print.Book;

//@AutoProtoSchemaBuilder(
//    includeClasses = {
//        EventFilterAdapter.class,
//        EventConverteAdapter.class
//    },
//    schemaFileName = "subs_cache.proto",
//    schemaFilePath = "proto/",
//    schemaPackageName = "subcache"
//)
@ProtoSchema(
    includeClasses = {
        EventFilterAdapter.class,
        EventConverteAdapter.class,
//        UUIDAdapter.class,
    },
    schemaFileName = "subs_cache.proto",
    schemaFilePath = "proto/",
    schemaPackageName = "subcache")
public interface SubsCacheSchema extends SerializationContextInitializer {
}
