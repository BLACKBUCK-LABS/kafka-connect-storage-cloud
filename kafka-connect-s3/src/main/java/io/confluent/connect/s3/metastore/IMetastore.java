package io.confluent.connect.s3.metastore;

import org.apache.kafka.connect.sink.SinkRecord;

public interface IMetastore  {
    public void updateMetastoreThroughCrawler(String tableName);
    public boolean isPartitionAvailable(String crawlerName, String partition);
    public void updateMetastoreThroughGlueSdk(String tableName, SinkRecord sinkRecord, String s3Path, String partition);
    public void createPartition(String name, String s3Path, String partition);
    }
