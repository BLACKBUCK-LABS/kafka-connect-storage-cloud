package io.confluent.connect.s3.metastore;

public interface IMetastore  {
    public void updateMetastore(String tableName);
}
