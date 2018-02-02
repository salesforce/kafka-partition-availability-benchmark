/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.Histogram;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;

class WriteTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(WriteTopic.class);

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");

    private static final Histogram topicCreateTimeSecs = Histogram
            .build("topicCreateTimeSecs", "Topic create time in ms")
            .register();
    private static final Histogram firstMessageProduceTimeSecs = Histogram
            .build("firstMessageProduceTimeSecs", "First message produce latency time in ms")
            .register();
    private static final Histogram produceMessageTimeSecs = Histogram
            .build("produceMessageTimeSecs", "Time it takes to produce messages in ms")
            .register();

    private final int topicId;
    private final String key;
    private final AdminClient kafkaAdminClient;
    private final short replicationFactor;
    private final KafkaProducer<Integer, Integer> kafkaProducer;

    public WriteTopic(int topicId, String key, AdminClient kafkaAdminClient, short replicationFactor,
                      KafkaProducer<Integer, Integer> kafkaProducer) {
        this.topicId = topicId;
        this.key = key;
        this.kafkaAdminClient = kafkaAdminClient;
        this.replicationFactor = replicationFactor;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public Exception call() {
        String topicName = TopicName.createTopicName(key, topicId);

        try {
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

            // Produce one message to "warm" kafka up
            firstMessageProduceTimeSecs.time(() -> {
                kafkaProducer.send(new ProducerRecord<>(topicName, topicId, -1));
            });
            log.debug("Produced first message to topic {}", topicName);

            int numMessagesToSend = 120;
            produceMessageTimeSecs.time(() -> {
                // TODO: Get this from properties
                for (int i = 0; i < numMessagesToSend; i++) {
                    kafkaProducer.send(new ProducerRecord<>(topicName, topicId, i));
                    log.debug("{}: Produced message {}", formatter.format(new Date()), topicId);
                }
            });
            log.debug("Produce {} messages to topic {}", numMessagesToSend, topicName);

            // TODO: Also keep producers around and periodically publish new messages
            return null;
        } catch (Exception e) {
            log.error("Failed to produce for topic {}", topicName, e);
            return e;
        }
    }
}
