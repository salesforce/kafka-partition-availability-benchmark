/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

class ConsumeTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTopic.class);

    private final int topicId;
    private final String key;
    private final int readWriteInterval;
    private final AdminClient kafkaAdminClient;
    private final Map<String, Object> kafkaConsumerConfig;
    private final short replicationFactor;
    private final boolean keepProducing;
    private final Timer consumerReceiveTimeNanos;
    private final Timer consumerCommitTimeNanos;
    private final Counter threadsAwaitingConsume;
    private final Counter threadsAwaitingCommit;
    private final Counter topicsAwaitingCreation;

    /**
     * @param topicId              Each topic gets a numeric id
     * @param key                  Prefix for topics created by this tool
     * @param readWriteInterval    How long should we wait before polls for consuming new messages
     * @param kafkaAdminClient
     * @param kafkaConsumerConfig
     * @param keepProducing        Whether we are continuously producing messages rather than just producing once
     * @param topicsAwaitingCreation
     */
    public ConsumeTopic(int topicId, String key, int readWriteInterval, AdminClient kafkaAdminClient,
                        Map<String, Object> kafkaConsumerConfig, short replicationFactor, boolean keepProducing,
                        Timer consumerReceiveTimeNanos, Timer consumerCommitTimeNanos,
                        Counter threadsAwaitingConsume, Counter threadsAwaitingCommit, Counter topicsAwaitingCreation) {
        this.topicId = topicId;
        this.key = key;
        this.readWriteInterval = readWriteInterval;
        this.kafkaAdminClient = kafkaAdminClient;
        this.kafkaConsumerConfig = Collections.unmodifiableMap(kafkaConsumerConfig);
        this.replicationFactor = replicationFactor;
        this.keepProducing = keepProducing;
        this.consumerReceiveTimeNanos = consumerReceiveTimeNanos;
        this.consumerCommitTimeNanos = consumerCommitTimeNanos;
        this.threadsAwaitingConsume = threadsAwaitingConsume;
        this.threadsAwaitingCommit = threadsAwaitingCommit;
        this.topicsAwaitingCreation = topicsAwaitingCreation;
    }

    @Override
    public Exception call() {
        String topicName = TopicName.createTopicName(key, topicId);
        try {
            TopicVerifier.checkTopic(kafkaAdminClient, topicName, replicationFactor, topicsAwaitingCreation);

            Map<String, Object> consumerConfigForTopic = new HashMap<>(kafkaConsumerConfig);
            consumerConfigForTopic.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
            KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerConfigForTopic);
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            consumer.assign(Collections.singleton(topicPartition));
            consumer.seekToBeginning(Collections.singleton(topicPartition));

            threadsAwaitingConsume.increment();
            while (true) {
                ConsumerRecords<Integer, byte[]> messages = consumer.poll(Duration.ofMillis(100));
                if (messages.count() == 0) {
                    log.debug("No messages detected on {}", topicName);
                    continue;
                }
                threadsAwaitingConsume.increment(-1);

                AtomicLong lastOffset = new AtomicLong();
                log.debug("Consuming {} records", messages.records(topicPartition).size());
                messages.records(topicPartition).forEach(consumerRecord -> {
                            consumerReceiveTimeNanos.record(Duration.ofMillis(System.currentTimeMillis() - consumerRecord.timestamp()));
                            lastOffset.set(consumerRecord.offset());
                        });

                threadsAwaitingCommit.increment();
                consumerCommitTimeNanos.record(() ->
                        consumer.commitSync(Collections.singletonMap(topicPartition,
                                new OffsetAndMetadata(lastOffset.get() + 1))));
                threadsAwaitingCommit.increment(-1);

                consumer.seek(topicPartition, lastOffset.get() + 1);

                ConsumerRecord<Integer, byte[]> lastMessage =
                        messages.records(topicPartition).get(messages.count() - 1);

                log.debug("Last consumed message {} -> {}..., consumed {} messages, topic: {}",
                        lastMessage.key(), new String(lastMessage.value()).substring(0, 15), messages.count(), topicName);
                threadsAwaitingConsume.increment();
            }
        } catch (Exception e) {
            log.error("Failed consume", e);
            return new Exception("Failed consume on topicName " + topicId, e);
        }

    }
}
