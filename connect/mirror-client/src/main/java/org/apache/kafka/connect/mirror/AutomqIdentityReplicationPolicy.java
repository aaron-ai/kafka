package org.apache.kafka.connect.mirror;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AutomqIdentityReplicationPolicy is a custom implementation of the ReplicationPolicy interface that allows for the
 * configuration of the offset-syncs-topic, checkpoints-topic, and heartbeats-topic via environment variables.
 * <p>
 * See more details from KIP-690
 */
public class AutomqIdentityReplicationPolicy extends IdentityReplicationPolicy {
    private static final Logger log = LoggerFactory.getLogger(AutomqIdentityReplicationPolicy.class);

    private static final String OFFSET_SYNC_TOPIC_ENV_KEY = "offset-syncs-topic";
    private static final String CHECKPOINTS_TOPIC_ENV_KEY = "checkpoints-topic";
    private static final String HEARTBEATS_TOPIC_ENV_KEY = "heartbeats-topic";

    @Override
    public String offsetSyncsTopic(String clusterAlias) {
        String offsetSyncsTopic = System.getenv(OFFSET_SYNC_TOPIC_ENV_KEY);
        if (offsetSyncsTopic == null) {
            return super.offsetSyncsTopic(clusterAlias);
        }
        log.info("Using offset syncs topic: {}", offsetSyncsTopic);
        return offsetSyncsTopic;
    }

    @Override
    public String checkpointsTopic(String clusterAlias) {
        String checkpointsTopic = System.getenv(CHECKPOINTS_TOPIC_ENV_KEY);
        if (checkpointsTopic == null) {
            return super.checkpointsTopic(clusterAlias);
        }
        log.info("Using checkpoints topic: {}", checkpointsTopic);
        return checkpointsTopic;
    }

    @Override
    public String heartbeatsTopic() {
        String heartbeatsTopic = System.getenv(HEARTBEATS_TOPIC_ENV_KEY);
        if (heartbeatsTopic == null) {
            return super.heartbeatsTopic();
        }
        log.info("Using heartbeats topic: {}", heartbeatsTopic);
        return heartbeatsTopic;
    }
}
