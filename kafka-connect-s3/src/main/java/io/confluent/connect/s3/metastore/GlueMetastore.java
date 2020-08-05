package io.confluent.connect.s3.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.*;
import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GlueMetastore implements IMetastore{
    private static final Logger log = LoggerFactory.getLogger(GlueMetastore.class);
    private final AWSGlue awsGlue;
    private final S3SinkConnectorConfig conf;
    private final String crawlerNameDelim = "\\.";
    private final String partitionDelim = "/";
    private final String partitonKeyValueDelim = "=";


    public GlueMetastore(S3SinkConnectorConfig conf) {
        this.conf = conf;
        awsGlue = AWSGlueClient.builder().withRegion(conf.getRegionConfig()).build();

    }

    @Override
    public void updateMetastore(String tableName) {
        try {
            StartCrawlerResult startCrawlerResult = awsGlue
                    .startCrawler(new StartCrawlerRequest().withName(tableName));
            String requestId = startCrawlerResult.getSdkResponseMetadata().getRequestId();
            log.info("Running Glue crawler, name : {}, requestId = {}", tableName, requestId);
        } catch (Exception e) {
            log.error("Got exception while running crawler, e = {}", e);
        }

    }


    @Override
    public boolean isPartitionAvailable(String crawlerName, String encodedPartition) {
        String dbName = crawlerName.split(crawlerNameDelim)[0];
        String tableName = crawlerName.replaceAll(crawlerNameDelim, "_");


        String[] partitions = encodedPartition.split(partitionDelim);
        List<String> partitionValues = new ArrayList<>();
        for(String partition: partitions) {
            partitionValues.add(partition.split(partitonKeyValueDelim)[1]);
        }
        GetPartitionRequest getPartitionRequest = new GetPartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues);
        try {
            GetPartitionResult getPartitionResult = awsGlue.getPartition(getPartitionRequest);
            return true;
        } catch (AWSGlueException e) {
            log.info("error while fetching partition , e ={}", e);

        }
        return false;

    }
}
