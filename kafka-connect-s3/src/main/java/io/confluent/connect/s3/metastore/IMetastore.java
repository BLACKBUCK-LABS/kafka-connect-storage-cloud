package io.confluent.connect.s3.metastore;

public interface IMetastore  {
    public void updateMetastore(String tableName);
    public boolean isPartitionAvailable(String crawlerName, String partition);


    }
