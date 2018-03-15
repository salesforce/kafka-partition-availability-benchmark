/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

class WriteTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(WriteTopic.class);

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");

    private static final Histogram firstMessageProduceTimeSecs = Histogram
            .build("firstMessageProduceTimeSecs", "First message produce latency time in ms")
            .register();
    private static final Histogram produceMessageTimeSecs = Histogram
            .build("produceMessageTimeSecs", "Time it takes to produce messages in ms")
            .register();
    private static final Gauge threadsAwaitingMessageProduce = Gauge.build("threadsAwaitingMessageProduce",
            "Number of threads that are that are waiting for message batch to be produced").register();

    private final int topicId;
    private final String key;
    private final AdminClient kafkaAdminClient;
    private final short replicationFactor;
    private final KafkaProducer<Integer, Integer> kafkaProducer;
    private final int numMessagesToSendPerBatch;
    private final boolean keepProducing;
    private final int readWriteInterval;

    /**
     * Produce messages thread constructor
     *
     * @param topicId                   Unique identifier for topic
     * @param key                       Key for the environment
     * @param kafkaAdminClient
     * @param replicationFactor         Kafka's replication factor for messages
     * @param numMessagesToSendPerBatch Number of messages to produce continously
     * @param keepProducing             Whether we should produce one message only or keep produce thread alive and
     *                                  produce each readWriteInterval
     * @param kafkaProducer
     * @param readWriteInterval         How long to wait between message production
     */
    public WriteTopic(int topicId, String key, AdminClient kafkaAdminClient, short replicationFactor,
                      int numMessagesToSendPerBatch, boolean keepProducing,
                      KafkaProducer<Integer, Integer> kafkaProducer, int readWriteInterval) {
        this.topicId = topicId;
        this.key = key;
        this.kafkaAdminClient = kafkaAdminClient;
        this.replicationFactor = replicationFactor;
        this.numMessagesToSendPerBatch = numMessagesToSendPerBatch;
        this.keepProducing = keepProducing;
        this.kafkaProducer = kafkaProducer;
        this.readWriteInterval = readWriteInterval;
    }

    @Override
    public Exception call() {
        String topicName = TopicName.createTopicName(key, topicId);

        try {
            TopicVerifier.checkTopic(kafkaAdminClient, topicName, replicationFactor);

            // Produce one message to "warm" kafka up
            threadsAwaitingMessageProduce.inc();
            firstMessageProduceTimeSecs.time(() ->
                    kafkaProducer.send(new ProducerRecord<>(topicName, topicId, -1)));
            log.debug("Produced first message to topic {}", topicName);


            while (keepProducing) {
                produceMessageTimeSecs.time(() -> {
                    // TODO: Get this from properties
                    for (int i = 0; i < numMessagesToSendPerBatch; i++) {
                        kafkaProducer.send(new ProducerRecord<>(topicName, topicId, i));
                        log.debug("{}: Produced message {}", formatter.format(new Date()), topicId);
                    }
                });
                threadsAwaitingMessageProduce.dec();
                Thread.sleep(readWriteInterval);
                threadsAwaitingMessageProduce.inc();
            }
            log.debug("Produce {} messages to topic {}", numMessagesToSendPerBatch, topicName);

            // TODO: Also keep producers around and periodically publish new messages
            return null;
        } catch (Exception e) {
            log.error("Failed to produce for topic {}", topicName, e);
            return e;
        }
    }
}
