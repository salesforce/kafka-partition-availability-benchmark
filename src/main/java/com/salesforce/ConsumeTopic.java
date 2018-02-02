/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.Histogram;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

class ConsumeTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTopic.class);

    private static final Histogram consumerReceiveTimeSecs = Histogram
            .build("consumerReceiveTimeSecs", "Time taken to do consumer.poll")
            .register();
    private static final Histogram consumerCommitTimeSecs = Histogram
            .build("consumerCommitTimeSecs", "Time it takes to commit new offset")
            .register();

    private final int topicId;
    private final String key;
    private final int consumerPollInterval;
    private final AdminClient kafkaAdminClient;
    private final Map<String, Object> kafkaConsumerConfig;
    private final short replicationFactor;

    /**
     * @param topicId Each topic gets a numeric id
     * @param key Prefix for topics created by this tool
     * @param consumerPollInterval How long should we wait before polls for new messages?
     * @param kafkaAdminClient
     * @param kafkaConsumerConfig
     */
    public ConsumeTopic(int topicId, String key, int consumerPollInterval, AdminClient kafkaAdminClient,
                        Map<String, Object> kafkaConsumerConfig, short replicationFactor) {
        this.topicId = topicId;
        this.key = key;
        this.consumerPollInterval = consumerPollInterval;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConsumerConfig = Collections.unmodifiableMap(kafkaConsumerConfig);
        this.replicationFactor = replicationFactor;
    }

    @Override
    public Exception call() {
        String topicName = TopicName.createTopicName(key, topicId);
        try {
            TopicVerifier.checkTopic(kafkaAdminClient, topicName, replicationFactor);

            Map<String, Object> consumerConfigForTopic = new HashMap<>(kafkaConsumerConfig);
            consumerConfigForTopic.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
            KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(consumerConfigForTopic);
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(topicPartition));

            AtomicInteger numMessages = new AtomicInteger();
            while (true) {
                ConsumerRecords<Integer, Integer> messages;
                Histogram.Timer consumerReceiveTimer = consumerReceiveTimeSecs.startTimer();
                try {
                    messages = consumer.poll(0);
                } finally {
                    consumerReceiveTimer.observeDuration();
                }
                if (messages.count() == 0) {
                    log.debug("Ran out of messages to process for topic {}; starting from beginning", topicName);
                    consumer.seekToBeginning(Collections.singleton(topicPartition));
                    numMessages.set(0);
                    Thread.sleep(consumerPollInterval);
                    continue;
                }
                numMessages.addAndGet(messages.count());

                consumerCommitTimeSecs.time(consumer::commitSync);

                ConsumerRecord<Integer, Integer> lastMessage =
                        messages.records(topicPartition).get(messages.count() - 1);

                log.debug("Last consumed message {}:{}, consumed {} messages, topic: {}",
                        lastMessage.key(), lastMessage.value(), messages.count(), topicName);
                Thread.sleep(consumerPollInterval);
            }
        } catch (Exception e) {
            log.error("Failed consume", e);
            return new Exception("Failed consume on topicName " + topicId, e);
        }

    }
}
