package io.confluent.connect.s3.metastore;

import org.apache.kafka.connect.sink.SinkRecord;

public interface IMetastore  {
    public void updateMetastore(String tableName);
    public boolean isPartitionAvailable(String crawlerName, String partition);
    public void updateMetastore(String tableName, SinkRecord sinkRecord);
    }
