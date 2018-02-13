package com.salesforce;

import io.prometheus.client.Histogram;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Callable;

public class CreateTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(CreateTopic.class);

    private static final Histogram topicCreateTimeSecs = Histogram
            .build("topicCreateTimeSecs", "Topic create time in ms")
            .register();

    private final int topicId;
    private final String key;
    private final AdminClient kafkaAdminClient;
    private final short replicationFactor;

    public CreateTopic(int topicId, String key, AdminClient kafkaAdminClient, short replicationFactor) {
        this.topicId = topicId;
        this.key = key;
        this.kafkaAdminClient = kafkaAdminClient;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Exception call() throws Exception {
        String topicName = TopicName.createTopicName(key, topicId);

        // TODO: Allow numPartitions to be changed
        Set<NewTopic> topic = Collections.singleton(new NewTopic(topicName, 1, replicationFactor));
        kafkaAdminClient.createTopics(topic);

        // Wait for topic to be created and for leader election to happen
        topicCreateTimeSecs.time(() -> {
            try {
                TopicVerifier.checkTopic(kafkaAdminClient, topicName, replicationFactor);
            } catch (InterruptedException e) {
                log.error("Unable to record topic creation", e);
            }
        });


        log.debug("Created topic {}", topic);
        return null;
    }
}
