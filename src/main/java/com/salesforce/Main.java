/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.statsd.StatsdMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void initGlobalMetricsRegistry() {
        MeterRegistry registry = new StatsdMeterRegistry(getStr -> null, Clock.SYSTEM);
        Metrics.globalRegistry.add(registry);
    }

    public static void main(String[] args) throws Exception {
        org.apache.log4j.Logger.getLogger("kafka").setLevel(Level.WARN);
        initGlobalMetricsRegistry();

        // Topic creates
        final Timer topicCreateTimeNanos = Timer
                .builder("topicCreateTimeNanos")
                .description("Topic create time in nanos")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
                .minimumExpectedValue(Duration.ofNanos(10))
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(Metrics.globalRegistry);
        final Counter topicsAwaitingCreation = Counter.builder("topicsAwaitingCreation")
                .description("Number of threads that are that are waiting for topics created and " +
                        "have leaders elected for said topics")
                .register(Metrics.globalRegistry);

        // Message produce
        final Timer firstMessageProduceTimeNanos = Timer
                .builder("firstMessageProduceTimeNanos")
                .description("First message produce latency time in nanos")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
                .minimumExpectedValue(Duration.ofNanos(10))
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(Metrics.globalRegistry);
        final Timer produceMessageTimeNanos = Timer
                .builder("produceMessageTimeNanos")
                .description("Time it takes to produce messages in nanos")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(Metrics.globalRegistry);
        final Counter threadsAwaitingMessageProduce = Counter
                .builder("threadsAwaitingMessageProduce")
                .description("Number of threads that are that are waiting for message batch to be produced")
                .register(Metrics.globalRegistry);

        // Message consume
        final Timer consumerReceiveTimeNanos = Timer
                .builder("consumerReceiveTimeNanos")
                .description("Time taken to do consumer.poll")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
                .minimumExpectedValue(Duration.ofNanos(10))
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(Metrics.globalRegistry);
        final Timer consumerCommitTimeNanos = Timer
                .builder("consumerCommitTimeNanos")
                .description( "Time it takes to commit new offset")
                .publishPercentiles(0.5, 0.95, 0.99, 0.999, 0.9999)
                .minimumExpectedValue(Duration.ofNanos(10))
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(Metrics.globalRegistry);
        final Counter threadsAwaitingConsume = Counter.builder("threadsAwaitingConsume")
                .description("Number of threads that are that are waiting for message batch to be consumed")
                .register(Metrics.globalRegistry);
        final Counter threadsAwaitingCommit = Counter.builder("threadsAwaitingCommit")
                .description("Number of threads that are that are waiting for message batch to be committed")
                .register(Metrics.globalRegistry);

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
        if (numConcurrentTopicCreations > numTopics) {
            log.error("You cannot concurrently create more topics than desired");
            System.exit(4);
        }
        String topicPrefix = settings.getProperty("default_topic_prefix");
        int readWriteIntervalMs = Integer.parseInt(settings.getProperty("read_write_interval_ms"));

        int numMessagesToSendPerBatch = Integer.parseInt(settings.getProperty("messages_per_batch"));

        boolean keepProducing = Boolean.parseBoolean(settings.getProperty("keep_producing"));

        // Admin settings
        Map<String, Object> kafkaAdminConfig = new HashMap<>();
        kafkaAdminConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getProperty("kafka.brokers"));

        // Consumer settings
        Map<String, Object> kafkaConsumerConfig = new HashMap<>(kafkaAdminConfig);
        kafkaConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, settings.getProperty("kafka.enable.auto.commit"));

        // Producer settings
        short replicationFactor = Short.parseShort(settings.getProperty("kafka.replication.factor"));
        String kafkaAcks = settings.getProperty("kafka.producer.acks");

        Map<String, Object> kafkaProducerConfig = new HashMap<>(kafkaAdminConfig);
        kafkaProducerConfig.put(ProducerConfig.ACKS_CONFIG, kafkaAcks);
        kafkaProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        kafkaProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Global counters
        Counter topicsCreated = Counter
                .builder("numTopicsCreated")
                .description("Number of topics we've attempted to create")
                .register(Metrics.globalRegistry);
        Counter topicsCreateFailed = Counter
                .builder("numTopicsCreateFailed")
                .description("Number of topics we've failed to create")
                .register(Metrics.globalRegistry);
        Counter topicsProduced = Counter
                .builder("numTopicsProduced")
                .description("Number of topics we've attempted to produce to")
                .register(Metrics.globalRegistry);
        Counter topicsProduceFailed = Counter
                .builder("numTopicsProduceFailed")
                .description("Number of topics we've failed to produce to")
                .register(Metrics.globalRegistry);
        Counter topicsConsumed = Counter
                .builder("numTopicsConsumed")
                .description("Number of topics we've attempted to consume from")
                .register(Metrics.globalRegistry);
        Counter topicsConsumeFailed = Counter
                .builder("numTopicsConsumeFailed")
                .description("Number of topics we've failed to consume from")
                .register(Metrics.globalRegistry);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> printMetrics(topicsCreated, topicsAwaitingCreation,
                firstMessageProduceTimeNanos, produceMessageTimeNanos,
                threadsAwaitingMessageProduce, consumerReceiveTimeNanos,
                consumerCommitTimeNanos, threadsAwaitingConsume,
                threadsAwaitingCommit)));

        try (AdminClient kafkaAdminClient = KafkaAdminClient.create(kafkaAdminConfig);
             KafkaProducer<Integer, byte[]> kafkaProducer = new KafkaProducer<>(kafkaProducerConfig)) {
            ExecutorService createTopics = Executors.newFixedThreadPool(numConcurrentTopicCreations);
            ExecutorService writeTopics = null;
            if (numConcurrentProducers > 0) {
                writeTopics = Executors.newFixedThreadPool(numConcurrentProducers);
            }
            ExecutorService consumeTopics = null;
            if (numConcurrentConsumers > 0) {
                consumeTopics = Executors.newFixedThreadPool(numConcurrentConsumers);
            }
            ExecutorService printMetrics = Executors.newSingleThreadExecutor();
            printMetrics.submit((Runnable) () -> {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted print metrics thread");
                    }
                    printMetrics(topicsCreated, topicsAwaitingCreation,
                            firstMessageProduceTimeNanos, produceMessageTimeNanos,
                            threadsAwaitingMessageProduce, consumerReceiveTimeNanos,
                            consumerCommitTimeNanos, threadsAwaitingConsume,
                            threadsAwaitingCommit);
                }
            });

            BlockingQueue<Future<Exception>> createTopicFutures = new ArrayBlockingQueue<>(numConcurrentTopicCreations);
            BlockingQueue<Future<Exception>> writeFutures = null;
            if (writeTopics != null) {
                writeFutures = new ArrayBlockingQueue<>(numConcurrentProducers);

            }
            BlockingQueue<Future<Exception>> consumerFutures = null;
            if (consumeTopics != null) {
                consumerFutures = new ArrayBlockingQueue<>(numConcurrentConsumers);
            }

            log.info("Starting benchmark...");
            for (int topic = 1; topic <= numTopics; topic++) {
                createTopicFutures.put(createTopics.submit(new CreateTopic(topic, topicPrefix, kafkaAdminClient,
                        replicationFactor, topicCreateTimeNanos, topicsAwaitingCreation)));
                topicsCreated.increment();
                if (createTopicFutures.size() >= numConcurrentTopicCreations) {
                    log.info("Created {} topics, ensuring success before producing more...", numConcurrentTopicCreations);
                    clearQueue(createTopicFutures, topicsCreateFailed);
                }

                if (writeTopics != null && writeFutures != null) {
                    writeFutures.put(writeTopics.submit(new WriteTopic(topic, topicPrefix, kafkaAdminClient,
                            replicationFactor, numMessagesToSendPerBatch,
                            keepProducing, kafkaProducer, readWriteIntervalMs, firstMessageProduceTimeNanos,
                            produceMessageTimeNanos, threadsAwaitingMessageProduce, topicsAwaitingCreation)));
                    topicsProduced.increment();
                }


                if (consumeTopics != null && consumerFutures != null) {
                    consumerFutures.put(consumeTopics.submit(new ConsumeTopic(topic, topicPrefix, readWriteIntervalMs,
                            kafkaAdminClient, kafkaConsumerConfig, replicationFactor, keepProducing,
                            consumerReceiveTimeNanos, consumerCommitTimeNanos,
                            threadsAwaitingConsume, threadsAwaitingCommit, topicsAwaitingCreation)));
                    topicsConsumed.increment();
                    if (consumerFutures.size() >= numConcurrentConsumers) {
                        log.debug("Consumed {} topics, clearing queue before consuming more...", numConcurrentConsumers);
                        clearQueue(consumerFutures, topicsConsumeFailed);
                    }
                }

                if (writeFutures != null && writeFutures.size() >= numConcurrentProducers) {
                    log.info("Produced {} topics, ensuring success before producing more...", numConcurrentProducers);
                    clearQueue(writeFutures, topicsProduceFailed);
                }
            }

            createTopics.shutdown();
            try {
                clearQueue(writeFutures, topicsProduceFailed);
                clearQueue(consumerFutures, topicsConsumeFailed);
            } finally {
                try {
                    writeTopics.shutdownNow();
                } finally {
                    consumeTopics.shutdownNow();
                }

            }
        }
    }

    private static void printMetrics(Counter topicsCreated, Counter topicsAwaitingCreation,
                                     Timer firstMessageProduceTimeNanos, Timer produceMessageTimeNanos,
                                     Counter threadsAwaitingMessageProduce, Timer consumerReceiveTimeNanos,
                                     Timer consumerCommitTimeNanos, Counter threadsAwaitingConsume,
                                     Counter threadsAwaitingCommit) {
        log.info("Stopping printing current accumulated metrics");
        log.info("Topics created: {}", topicsCreated.count());
        log.info("Topics awaiting creation: {}", topicsAwaitingCreation.count());
        log.info("Thread awaiting produce: {}", threadsAwaitingMessageProduce.count());
        log.info("Thread awaiting commit: {}", threadsAwaitingCommit.count());
        log.info("Thread awaiting consume: {}", threadsAwaitingConsume.count());
        log.info("Produced num: {}", produceMessageTimeNanos.count());
        log.info("Produce percentiles: {}", Arrays.stream(produceMessageTimeNanos.takeSnapshot().percentileValues())
                .map(valueAtPercentile ->
                        String.format("%sms at %s%%",
                                (double) TimeUnit.NANOSECONDS.toMicros(
                                    Double.valueOf(valueAtPercentile.value()).longValue()
                                ) / 1000,
                                valueAtPercentile.percentile() * 100))
                .collect(Collectors.joining(" ")));
        //firstMessageProduceTimeNanos.takeSnapshot().outputSummary(System.out, 0);
        //produceMessageTimeNanos.takeSnapshot().outputSummary(System.out, 0);
        log.info("Commit num: {}", consumerCommitTimeNanos.count());
        log.info("Commit percentiles: {}", Arrays.stream(consumerCommitTimeNanos.takeSnapshot().percentileValues())
                .map(valueAtPercentile ->
                        String.format("%sms at %s%%",
                                (double) TimeUnit.NANOSECONDS.toMicros(
                                        Double.valueOf(valueAtPercentile.value()).longValue()
                                ) / 1000,
                                valueAtPercentile.percentile() * 100))
                .collect(Collectors.joining(" ")));
        log.info("Consumed num: {}", consumerReceiveTimeNanos.count());
        log.info("E2E percentiles: {}", Arrays.stream(consumerReceiveTimeNanos.takeSnapshot().percentileValues())
                .map(valueAtPercentile ->
                        String.format("%sms at %s%%",
                                (double) TimeUnit.NANOSECONDS.toMicros(
                                        Double.valueOf(valueAtPercentile.value()).longValue()
                                ) / 1000,
                                valueAtPercentile.percentile() * 100))
                .collect(Collectors.joining(" ")));
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
                    failedCounter.increment();
                    log.error("Fatal error:", e);
                    throw new ExecutionException(e);
                }

                runningTally++;
            }
        }
        return runningTally;
    }
}
