package io.confluent.connect.s3.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.model.*;
import org.apache.avro.Schema;
import org.apache.commons.lang.ObjectUtils;
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
    public void updateMetastoreThroughCrawler(String name) {
        StartCrawlerResult startCrawlerResult = awsGlue.startCrawler(new StartCrawlerRequest().withName(name));
        String requestId = startCrawlerResult.getSdkResponseMetadata().getRequestId();
        log.info("Running Glue crawler, name : {}, requestId = {}", name, requestId);
    }

    @Override
    public void updateMetastoreThroughGlueSdk(String name, SinkRecord sinkRecord, String s3Path, String partition){
        String[] parts = name.split("\\.");
        List<Column>partitionKeys= getPartitionKeysUsingPartition(partition);
        String databaseName=parts[0];
        String tableName = name.replace(".", "_");
        List<Column> columns = getListOfColumns(sinkRecord);
        StorageDescriptor storageDescriptor = getDefaultStorageDescriptor();
        storageDescriptor.setColumns(columns);
        storageDescriptor.setLocation(buildS3Paths(s3Path,name));
        TableInput tableInput=new TableInput().withName(tableName).withPartitionKeys(partitionKeys)
                .withStorageDescriptor(storageDescriptor);
        Table table =checkIfTableExists(databaseName, tableName);
        if(table!=null){
            if(checkIfUpdateRequired(table, tableInput)){
                log.info("Table exists, updating table {}", tableName);
                awsGlue.updateTable(new UpdateTableRequest().withDatabaseName(databaseName)
                        .withTableInput(tableInput));
            }

        } else {
            log.info("Creating new table {}", tableName);
            awsGlue.createTable(new CreateTableRequest().withDatabaseName(databaseName).withTableInput(tableInput));
        }
    }

    private Boolean checkIfUpdateRequired(Table table, TableInput tableInput) {
        log.info("Table {}", table);
        if (tableInput.getStorageDescriptor().getColumns().size() != table.getStorageDescriptor().getColumns().size()
                || tableInput.getPartitionKeys().size() != table.getPartitionKeys().size()) {
            return true;
        }
        if (!table.getStorageDescriptor().getLocation().equals(tableInput.getStorageDescriptor().getLocation())) {
            return true;
        }
        Map<String, Column> columnMap = table.getStorageDescriptor().getColumns().stream()
                .collect(Collectors.toMap(Column::getName, e -> e));
        Map<String, Column> partitionKeyMap = table.getPartitionKeys().stream()
                .collect(Collectors.toMap(Column::getName, e -> e));
        for (Column column : tableInput.getStorageDescriptor().getColumns()) {
            Column existingColumn = columnMap.getOrDefault(column.getName(), null);
            if (existingColumn == null || !existingColumn.getType().equals(column.getType())) {
                return true;
            }
        }
        for (Column column : tableInput.getPartitionKeys()) {
            Column existingPartitionKey = partitionKeyMap.getOrDefault(column.getName(), null);
            if (existingPartitionKey == null || existingPartitionKey.getType().equals(column.getType())) {
                return true;
            }
        }
        log.info("No changes in glue table");
        return false;
    }

    private List<Column> getPartitionKeysUsingPartition(String partition) {
        String[] partitionSplit = partition.split(partitionDelim);

        List<Column> keys = new ArrayList<>();
        for (String partitionKey : partitionSplit) {
            String[] keyValue = partitionKey.split(partitonKeyValueDelim);
            String key = keyValue[0];
            keys.add(new Column().withName(key).withType("string"));
        }
        return keys;
    }

    private List<String> getPartitionValuesUsingPartition(String partition) {
        String[] partitionSplit = partition.split(partitionDelim);

        List<String> values = new ArrayList<>();
        for (String partitionKey : partitionSplit) {
            String[] keyValue = partitionKey.split(partitonKeyValueDelim);
            String value = keyValue[1];
            values.add(value);
        }
        return values;
    }

    private String buildS3Paths(String s3BucketName, String path) {
        return "s3://"+ s3BucketName + "/" + path+ "/";
    }
    private List<Column> getListOfColumns(SinkRecord sinkRecord) {
        List<Column> columns= new ArrayList<>();
        for (Field field: sinkRecord.valueSchema().fields()){
            log.info("schema values {}", field);
            Column column= new Column();
            column.setName(field.name());
            column.setType(GlueDataType.getDataType(field.schema().type()));
            columns.add(column);
        }
        return columns;
    }

    private Table checkIfTableExists(String databaseName, String tableName) {
        try {
            return awsGlue.getTable(new GetTableRequest()
                    .withDatabaseName(databaseName)
                    .withName(tableName)).getTable();
            // If the getTable request succeeds, the table exists
        } catch (Exception e) {
            if (e instanceof EntityNotFoundException){
                log.info("Entity does not exist");
                return null;
            }
            // If the getTable request throws a ResourceNotFoundException, the table does not exist
        }
        return null;
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

    public void createPartition(String name, String s3Path, String partition){
        String[] parts = name.split(topicNameDelim);
        String tableName = name.replace(".", "_");
        String databaseName=parts[0];
        CreatePartitionRequest createPartitionRequest= new CreatePartitionRequest();
        StorageDescriptor storageDescriptor= getDefaultStorageDescriptor();
        log.info("Creating partition bucket {}", buildS3Paths(s3Path, name)+partition+"/");
        storageDescriptor.setLocation(buildS3Paths(s3Path, name)+partition+"/");
        createPartitionRequest.setDatabaseName(databaseName);
        createPartitionRequest.setTableName(tableName);
        createPartitionRequest.setPartitionInput(
                new PartitionInput().withValues(getPartitionValuesUsingPartition(partition))
                        .withStorageDescriptor(storageDescriptor));
        awsGlue.createPartition(createPartitionRequest);
    }

    public StorageDescriptor getDefaultStorageDescriptor(){
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("serialization.format", "1");
        storageDescriptor.setSerdeInfo(
                new SerDeInfo().withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                        .withParameters(parameters));
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        return storageDescriptor;
    }
}