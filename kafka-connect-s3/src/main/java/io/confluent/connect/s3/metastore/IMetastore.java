package io.confluent.connect.s3.metastore;

import com.amazonaws.services.glue.model.GetTableVersionsRequest;
import com.amazonaws.services.glue.model.GetTableVersionsResult;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface IMetastore  {
    public void updateMetastoreThroughCrawler(String tableName);
    public boolean isPartitionAvailable(String crawlerName, String partition);
    public void updateMetastoreThroughGlueSdk(String tableName, SinkRecord sinkRecord, String s3Path, String partition);
    public void createPartition(String name, String s3Path, String partition);

    public void deleteExcessGlueTableVersions(String name);
}
