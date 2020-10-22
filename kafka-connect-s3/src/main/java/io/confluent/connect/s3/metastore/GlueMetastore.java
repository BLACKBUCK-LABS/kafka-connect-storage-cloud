package io.confluent.connect.s3.metastore;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.AWSGlueException;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartCrawlerResult;

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
    public boolean isPartitionAvailable(String topicName, String encodedPartition) {
        String dbName = topicName.split(topicNameDelim)[0];
        String tableName = topicName.replaceAll(topicNameDelim, "_");

        String[] partitions = encodedPartition.split(partitionDelim);
        List<String> partitionValues = new ArrayList<>();
        for (String partition : partitions) {
            partitionValues.add(partition.split(partitonKeyValueDelim)[1]);
        }
        GetPartitionRequest getPartitionRequest = new GetPartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withPartitionValues(partitionValues);
        try {
            awsGlue.getPartition(getPartitionRequest);
            return true;
        } catch (AWSGlueException e) {
            log.info("error while fetching partition , e = ", e);
        }
        return false;
    }
}