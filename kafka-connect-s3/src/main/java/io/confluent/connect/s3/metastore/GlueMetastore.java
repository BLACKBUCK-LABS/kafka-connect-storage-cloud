package io.confluent.connect.s3.metastore;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.glue.model.*;
import org.apache.avro.Schema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;

import io.confluent.connect.s3.S3SinkConnectorConfig;

public class GlueMetastore implements IMetastore {

    private static final Logger log = LoggerFactory.getLogger(GlueMetastore.class);
    private final AWSGlue awsGlue;
    private final String topicNameDelim = "\\.";
    private final String partitionDelim = "/";
    private final String partitonKeyValueDelim = "=";

    public GlueMetastore(S3SinkConnectorConfig conf) {
        awsGlue = AWSGlueClient.builder().withRegion(conf.getRegionConfig()).build();
    }

    @Override
    public void updateMetastore(String name) {
        StartCrawlerResult startCrawlerResult = awsGlue.startCrawler(new StartCrawlerRequest().withName(name));
        String requestId = startCrawlerResult.getSdkResponseMetadata().getRequestId();
        log.info("Running Glue crawler, name : {}, requestId = {}", name, requestId);
    }

    @Override
    public void updateMetastore(String name, SinkRecord sinkRecord){
        String[] parts = name.split("\\.");
        log.info("testing update metastore");

        String databaseName=parts[0];
        String tableName= name.replace(".", "_");
        List<Column> columns= getListOfColumns(sinkRecord);
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        storageDescriptor.setSerdeInfo(new SerDeInfo());
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        storageDescriptor.setColumns(columns);
//        storageDescriptor.setLocation(buildS3Paths(databaseName,name));
        if(checkIfTableExists(databaseName, tableName)){
            log.info("Table exists");
            awsGlue.updateTable(new UpdateTableRequest().withDatabaseName(databaseName)
                    .withTableInput(new TableInput().withName(tableName).withStorageDescriptor(storageDescriptor)));
        } else {
            log.info("Table does not exists");
            awsGlue.createTable(new CreateTableRequest().withDatabaseName(databaseName).withTableInput(
                    new TableInput().withName(tableName)
                            .withStorageDescriptor(storageDescriptor.withLocation(buildS3Paths(databaseName, name)))));
        }
    }

    private String buildS3Paths(String teamName, String path) {
        return "s3://zinka-datalake-mumbai/" + teamName + '/' + path;
    }
    private List<Column> getListOfColumns(SinkRecord sinkRecord) {
        List<Column> columns= new ArrayList<>();
        for (Field field: sinkRecord.valueSchema().fields()){
            Column column= new Column();
            column.setName(field.name());
            column.setType(field.schema().type().getName());
            columns.add(column);
        }
        return columns;
    }

    private boolean checkIfTableExists(String databaseName, String tableName) {
        boolean tableExists = false;
        try {

            awsGlue.getTable(new GetTableRequest()
                    .withDatabaseName(databaseName)
                    .withName(tableName));
            // If the getTable request succeeds, the table exists
            tableExists = true;
        } catch (Exception e) {
            if (e instanceof EntityNotFoundException){
                tableExists = false;
            }
            // If the getTable request throws a ResourceNotFoundException, the table does not exist
        }
        return tableExists;
    }

    @Override
    public boolean isPartitionAvailable(String topicName, String encodedPartition) {
        String glueDbName = topicName.split(topicNameDelim)[0];
        String glueTableName = topicName.replaceAll(topicNameDelim, "_");

        String[] partitions = encodedPartition.split(partitionDelim);
        List<String> partitionValues = new ArrayList<>();
        for (String partition : partitions) {
            partitionValues.add(partition.split(partitonKeyValueDelim)[1]);
        }
        GetPartitionRequest getPartitionRequest = new GetPartitionRequest().withDatabaseName(glueDbName)
                .withTableName(glueTableName).withPartitionValues(partitionValues);
        try {
            awsGlue.getPartition(getPartitionRequest);
            return true;
        } catch (AWSGlueException e) {
            log.info("error while fetching partition , e = ", e);
        }
        return false;
    }
}