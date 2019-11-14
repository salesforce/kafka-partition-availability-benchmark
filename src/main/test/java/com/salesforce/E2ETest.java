package com.salesforce;

import com.github.charithe.kafka.EphemeralKafkaCluster;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class E2ETest {
    @Test
    public void e2eTest() throws Exception {
        EphemeralKafkaCluster cluster = EphemeralKafkaCluster.create(1);
        long maxWaitMs = TimeUnit.MINUTES.toMillis(1);
        for (long wait = 0; wait < maxWaitMs; wait += TimeUnit.SECONDS.toMillis(1)) {
            if (cluster.isRunning() && cluster.isHealthy()) {
                break;
            } else {
                TimeUnit.SECONDS.sleep(1);
            }
        }
        while (true) {
            System.out.println(cluster.producerConfig());
            Thread.sleep(Duration.ofSeconds(10).toMillis());
        }
    }

}
