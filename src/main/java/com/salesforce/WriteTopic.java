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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class WriteTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(WriteTopic.class);

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");

    private final int topicId;
    private final String key;
    private final AdminClient kafkaAdminClient;
    private final short replicationFactor;
    private final KafkaProducer<Integer, byte[]> kafkaProducer;
    private final int numMessagesToSendPerBatch;
    private final boolean keepProducing;
    private final int readWriteInterval;
    private final Timer firstMessageProduceTimeNanos;
    private final Timer produceMessageTimeNanos;
    private final Counter threadsAwaitingMessageProduce;
    private final Counter topicsAwaitingCreation;

    /**
     * Produce messages thread constructor
     *
     * @param topicId                       Unique identifier for topic
     * @param key                           Key for the environment
     * @param kafkaAdminClient
     * @param replicationFactor             Kafka's replication factor for messages
     * @param numMessagesToSendPerBatch     Number of messages to produce continuously
     * @param keepProducing                 Whether we should produce one message only or keep produce thread alive and
     *                                      produce each readWriteInterval
     * @param kafkaProducer
     * @param readWriteInterval             How long to wait between message production
     * @param firstMessageProduceTimeNanos
     * @param produceMessageTimeNanos
     * @param threadsAwaitingMessageProduce
     * @param topicsAwaitingCreation
     */
    public WriteTopic(int topicId, String key, AdminClient kafkaAdminClient, short replicationFactor,
                      int numMessagesToSendPerBatch, boolean keepProducing,
                      KafkaProducer<Integer, byte[]> kafkaProducer, int readWriteInterval,
                      Timer firstMessageProduceTimeNanos, Timer produceMessageTimeNanos,
                      Counter threadsAwaitingMessageProduce, Counter topicsAwaitingCreation) {
        this.topicId = topicId;
        this.key = key;
        this.kafkaAdminClient = kafkaAdminClient;
        this.replicationFactor = replicationFactor;
        this.numMessagesToSendPerBatch = numMessagesToSendPerBatch;
        this.keepProducing = keepProducing;
        this.kafkaProducer = kafkaProducer;
        this.readWriteInterval = readWriteInterval;
        this.firstMessageProduceTimeNanos = firstMessageProduceTimeNanos;
        this.produceMessageTimeNanos = produceMessageTimeNanos;
        this.threadsAwaitingMessageProduce = threadsAwaitingMessageProduce;
        this.topicsAwaitingCreation = topicsAwaitingCreation;
    }

    @Override
    public Exception call() {
        String topicName = TopicName.createTopicName(key, topicId);

        try {
            TopicVerifier.checkTopic(kafkaAdminClient, topicName, replicationFactor, topicsAwaitingCreation);

            Map<String, String> messageData = new HashMap<>();
            // TODO: Tunable size of messages
            int sizeOfMessage = 512;
            char[] randomChars = new char[sizeOfMessage];
            Arrays.fill(randomChars, '9');
            messageData.put("Junk", String.valueOf(randomChars));
            final byte[] byteDataInit = new String(System.currentTimeMillis() + "" + new String(randomChars)).getBytes();


            // Produce one message to "warm" kafka up
            threadsAwaitingMessageProduce.increment();
            firstMessageProduceTimeNanos.record(() ->
                    kafkaProducer.send(new ProducerRecord<>(topicName, topicId, byteDataInit)));
            log.debug("Produced first message to topic {}", topicName);

            while (keepProducing) {
                // TODO: Get this from properties
                for (int i = 0; i < numMessagesToSendPerBatch; i++) {
                    int finalI = i;
                    final byte[] byteData = new String(System.currentTimeMillis() + "" + new String(randomChars)).getBytes();
                    produceMessageTimeNanos.record(() -> {
                        Future<RecordMetadata> produce =
                                kafkaProducer.send(new ProducerRecord<>(topicName, finalI, byteData));
                        kafkaProducer.flush();
                        try {
                            produce.get();
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("Failed to get record metadata after produce");
                        }
                    });
                    log.debug("{}: Produced message {}", formatter.format(new Date()), topicId);
                }
                threadsAwaitingMessageProduce.increment(-1);
                Thread.sleep(readWriteInterval);
                threadsAwaitingMessageProduce.increment();
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
