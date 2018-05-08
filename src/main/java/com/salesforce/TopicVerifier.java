/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

class TopicVerifier {
    private static final Logger log = LoggerFactory.getLogger(TopicVerifier.class);
    private static final Gauge topicsCreated = Gauge.build("topicsAwaitingCreation",
            "Number of threads that are that are waiting for topics created and have leaders elected for " +
                    "said topics").register();

    public static void checkTopic(AdminClient kafkaAdminClient, String topicName, short replicationFactor)
            throws InterruptedException {
        topicsCreated.inc();
        while (true) {
            DescribeTopicsResult result = kafkaAdminClient.describeTopics(Collections.singleton(topicName));
            Map<String, TopicDescription> descriptionMap;
            try {
                descriptionMap = result.all().get();
            } catch (Exception unknownTopic) {
                if (!unknownTopic.getMessage().contains("UnknownTopicOrPartitionException")) {
                    log.error("UnexpectedException; trying again...", unknownTopic);
                    Thread.sleep(1000);
                    checkTopic(kafkaAdminClient, topicName, replicationFactor);
                    break;
                }
                log.info("Topic {} not created yet, checking again in 1 second", topicName);
                Thread.sleep(1000);
                continue;
            }
            TopicPartitionInfo partition = descriptionMap.get(topicName).partitions().get(0);

            /*
            * If the replication factor is 2 or lower, we won't have more than 1 ISR - this is mostly
            * for local testing purposes
             */
            int isrSize = 1;
            if (replicationFactor <= 2) {
                isrSize = 0;
            }
            if (!partition.leader().isEmpty()
                    && partition.replicas().size() == replicationFactor
                    && partition.isr().size() > isrSize ) {
                topicsCreated.dec();
                break;
            } else {
                log.info("Topic hasn't elected leader yet, checking again in 1 sec");
                log.info("Leader: {} Replicas: {} ISR: {}", partition.leader(), partition.replicas(), partition.isr());
                Thread.sleep(1000);
            }
        }
    }
}
