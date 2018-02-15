# kafka-partition-availability-benchmark

This repository contains a Kafka partition stress test. The goal of it is to make it easier to validate changes to 
Kafka with respect how many concurrent replicated partitions it can support. 

We want to ensure that our Kafka users have the following gaurantees: 

1. Any topic or partition is available for consume or produce at any time
2. Latencies are below certain SLAs for consume and produce
3. Users can reset the consumer offset to begining and reconsume at any time

Given that Kafka currently (as of 02/02/18) doesn't limit the number of topics you are allowed to created, this tool
helped us answer, "how many topics and paritions can we place on our multitenant Kafka systems before things start 
going downhill?"

## Building

This will create a runnable jar in the target directory called `kafka_partition_availability_benchmark.jar`:

```
mvn package
```

## Configuration
You can see all configurable parameters in `src/main/resources/kafka-partition-availability-benchmark.properties`

The defaults are set to something you can run against a local single-broker installation of kafka. In most cases, you 
only probably need to set four things to put stress on a real test environment:
```
cat > ~/.kafka-partition-availability-benchmark.properties << EOF
kafka.replication.factor = 3
num_topics = 4000
num_concurrent_consumers = 4000
kafka.brokers = my_test_kafka:9092
EOF
```

Depending on how powerful your test runner host is, you might be able to bump up the number of topics past `4000`. In
our testing, `4000` was what an i3.2xlarge instance could bear before test results started to get skewed. 

To get past `4000` topics, we ran this tool on multiple test runners. We recommend setting the default topic prefix to 
something unique per test runner by doing something like this:
```
echo "default_topic_prefix = `hostname`" >> ~/.kafka-partition-availability-benchmark.properties
```

### Measuring produce and consume at the same time

By default, the benchmark will only produce one message per partition and re-consume that messages every second. To test produce continously in the same fashion and not 
rely on re-consuming the same messages, set the following configuration options:
```
cat > ~/.kafka-partition-availability-benchmark.properties << EOF
keep_producing = true
EOF
```

## Running

You can use your favorite configuration management tool such as Ansible to make the below more elegant. From a central 
host you can do something like this:

```
for test_runner in ${test_runners}; do
    rsync ~/.kafka-partition-availability-benchmark.properties target/kafka_partition_availability_benchmark.jar ${test_runner}:
    ssh ${test_runner} 'echo "default_topic_prefix = `hostname`" >> ~/.kafka-partition-availability-benchmark.properties'
    ssh ${test_runner} 'nohup java -jar kafka_partition_availability_benchmark.jar &> kafka_partition_availability_benchmark.log &';
    ssh ${test_runner} 'for pr in `pgrep -f kafka_partition_availability_benchmark`; do sudo prlimit -n50000:50000 -p $pr; done';
done
```
