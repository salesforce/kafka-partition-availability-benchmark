/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        Properties settings = new Properties();
        try (InputStream defaults = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("kafka-partition-availability-benchmark.properties")) {
            settings.load(defaults);
        }

        // If you have the properties file in your home dir apply those as overrides
        Path userPropFile = Paths.get(System.getProperty("user.home"), ".kafka-partition-availability-benchmark.properties");
        if (Files.exists(userPropFile)) {
            log.info("Found {}", userPropFile);
            try (InputStream userProps = new FileInputStream(userPropFile.toFile())) {
                settings.load(userProps);
            }
        }

        Executor metricsServer = Executors.newFixedThreadPool(1);
        metricsServer.execute(
                new PrometheusMetricsServer(Integer.valueOf(settings.getProperty("prometheus_metrics_port"))));
        Runtime.getRuntime().addShutdownHook(new Thread(CollectorRegistry.defaultRegistry::clear));

        Integer numConcurrentTopicCreations = Integer.valueOf(settings.getProperty("num_concurrent_topic_creations"));
        Integer numConcurrentConsumers = Integer.valueOf(settings.getProperty("num_concurrent_consumers"));
        Integer numConcurrentProducers = Integer.valueOf(settings.getProperty("num_concurrent_producers"));
        Integer numTopics = Integer.valueOf(settings.getProperty("num_topics"));
        if (numConcurrentConsumers > numTopics) {
            log.error("You must set num_topics higher than or same as num_concurrent_consumers");
            System.exit(1);
        }
        if (numConcurrentProducers > numTopics) {
            log.error("You must set num_topics higher than or same as num_concurrent_producers");
            System.exit(2);
        }
        if (numConcurrentProducers > numConcurrentConsumers) {
            log.error("Havoc will ensue if you have fewer concurrent producers than consumers");
            System.exit(3);
        }
        if (numConcurrentTopicCreations > numTopics) {
            log.error("You cannot concurrently create more topics than desired");
            System.exit(4);
        }
        String topicPrefix = settings.getProperty("default_topic_prefix");
        Integer readWriteIntervalMs = Integer.valueOf(settings.getProperty("read_write_interval_ms"));

        Integer numMessagesToSendPerBatch = Integer.valueOf(settings.getProperty("messages_per_batch"));

        Boolean keepProducing = Boolean.valueOf(settings.getProperty("keep_producing"));

        // Admin settings
        Map<String, Object> kafkaAdminConfig = new HashMap<>();
        kafkaAdminConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getProperty("kafka.brokers"));

        // Consumer settings
        Map<String, Object> kafkaConsumerConfig = new HashMap<>(kafkaAdminConfig);
        kafkaConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, settings.getProperty("kafka.enable.auto.commit"));

        // Producer settings
        short replicationFactor = Short.valueOf(settings.getProperty("kafka.replication.factor"));
        String kafkaAcks = settings.getProperty("kafka.producer.acks");

        Map<String, Object> kafkaProducerConfig = new HashMap<>(kafkaAdminConfig);
        kafkaProducerConfig.put(ProducerConfig.ACKS_CONFIG, kafkaAcks);
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());

        // Global counters
        Counter topicsCreated = Counter
                .build("numTopicsCreated", "Number of topics we've attempted to create")
                .register();
        Counter topicsCreateFailed = Counter
                .build("numTopicsCreateFailed", "Number of topics we've failed to create")
                .register();
        Counter topicsProduced = Counter
                .build("numTopicsProduced", "Number of topics we've attempted to produce to")
                .register();
        Counter topicsProduceFailed = Counter
                .build("numTopicsProduceFailed", "Number of topics we've failed to produce to")
                .register();
        Counter topicsConsumed = Counter
                .build("numTopicsConsumed", "Number of topics we've attempted to consume from")
                .register();
        Counter topicsConsumeFailed = Counter
                .build("numTopicsConsumeFailed", "Number of topics we've failed to consume from")
                .register();

        try (AdminClient kafkaAdminClient = KafkaAdminClient.create(kafkaAdminConfig);
             KafkaProducer<Integer, Integer> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig)) {
            ExecutorService createTopics = Executors.newFixedThreadPool(numConcurrentTopicCreations);
            ExecutorService writeTopics = Executors.newFixedThreadPool(numConcurrentProducers);
            ExecutorService consumeTopics = Executors.newFixedThreadPool(numConcurrentConsumers);

            BlockingQueue<Future<Exception>> createTopicFutures = new ArrayBlockingQueue<>(numConcurrentTopicCreations);
            BlockingQueue<Future<Exception>> writeFutures = new ArrayBlockingQueue<>(numConcurrentProducers);
            BlockingQueue<Future<Exception>> consumerFutures = new ArrayBlockingQueue<>(numConcurrentConsumers);

            log.info("Starting benchmark...");
            for (int topic = 1; topic <= numTopics; topic++) {
                createTopicFutures.put(createTopics.submit(new CreateTopic(topic, topicPrefix, kafkaAdminClient,
                        replicationFactor)));
                topicsCreated.inc();
                if (createTopicFutures.size() >= numConcurrentTopicCreations) {
                    log.info("Created {} topics, ensuring success before producing more...", numConcurrentTopicCreations);
                    clearQueue(createTopicFutures, topicsCreateFailed);
                }

                writeFutures.put(writeTopics.submit(new WriteTopic(topic, topicPrefix, kafkaAdminClient,
                        replicationFactor, numMessagesToSendPerBatch,
                        keepProducing, kafkaProducer, readWriteIntervalMs)));
                topicsProduced.inc();
                if (writeFutures.size() >= numConcurrentProducers) {
                    log.info("Produced {} topics, ensuring success before producing more...", numConcurrentProducers);
                    clearQueue(writeFutures, topicsProduceFailed);
                }

                consumerFutures.put(consumeTopics.submit(new ConsumeTopic(topic, topicPrefix, readWriteIntervalMs,
                        kafkaAdminClient, kafkaConsumerConfig, replicationFactor, keepProducing)));
                topicsConsumed.inc();
                if (consumerFutures.size() >= numConcurrentConsumers) {
                    log.debug("Consumed {} topics, clearing queue before consuming more...", numConcurrentConsumers);
                    clearQueue(consumerFutures, topicsConsumeFailed);
                }
            }

            createTopics.shutdown();
            try {
                clearQueue(writeFutures, topicsProduceFailed);
                clearQueue(consumerFutures, topicsConsumeFailed);
            } finally {
                consumeTopics.shutdownNow();
            }
        }
    }

    private static int clearQueue(BlockingQueue<Future<Exception>> futures, Counter failedCounter)
            throws InterruptedException, ExecutionException {
        int runningTally = 0;
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                Future<Exception> f = futures.take();
                log.debug("Waiting for {} to close", f.toString());
                Exception e = f.get();
                if (e != null) {
                    failedCounter.inc();
                    log.error("Fatal error:", e);
                    throw new ExecutionException(e);
                }

                runningTally++;
            }
        }
        return runningTally;
    }
}
