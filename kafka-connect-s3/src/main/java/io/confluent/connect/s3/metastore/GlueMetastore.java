package io.confluent.connect.s3.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.StartCrawlerRequest;
import com.amazonaws.services.glue.model.StartCrawlerResult;
import com.amazonaws.services.s3.AmazonS3;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlueMetastore implements IMetastore{
    private static final Logger log = LoggerFactory.getLogger(GlueMetastore.class);
    private final AWSGlue awsGlue;
    private final S3SinkConnectorConfig conf;

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
}
