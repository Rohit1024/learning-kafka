# Apache Kafka Lessons

Follow these lessons to systematically master Apache Kafka. Each lesson covers a specific technical concept and includes a self-check quiz to reinforce retention.

## Available Lessons

1.  **[Lesson 1: Kafka Basics & Architecture Patterns](0001-kafka-basics-and-patterns.md)**
    *   Concepts: Broker, Topic, Partition, Offset, Consumer Groups.
    *   Patterns: Service Orchestration vs. Event Choreography.
2.  **[Lesson 2: Setting up Kafka (Local Cluster & Cloud)](0002-setting-up-kafka.md)**
    *   Local setup: Docker Compose with KRaft mode.
    *   Cloud comparisons: Confluent Cloud, AWS MSK, Upstash, Aiven.
3.  **[Lesson 3: Java Spring Boot Integration](0003-spring-boot-kafka.md)**
    *   Dependencies & properties configurations.
    *   Asynchronous producers using `KafkaTemplate`.
    *   Consumers with manual offset acknowledgments.
4.  **[Lesson 4: TypeScript with @platformatic/kafka](0004-typescript-kafka.md)**
    *   Node.js Kafka setup with TypeScript.
    *   Producing key-routed messages and handling consumers in asynchronous loops.
5.  **[Lesson 5: Operations, Delivery Guarantees & Troubleshooting](0005-operations-and-troubleshooting.md)**
    *   Managing and reducing consumer lag.
    *   Preventing rebalance storms (`max.poll.interval.ms`).
    *   Handling deserialization poison pills via Dead Letter Queues (DLQ).
6.  **[Lesson 6: Spring for Apache Kafka Streams](0006-spring-kafka-streams.md)**
    *   Kafka Streams setup, `StreamsBuilder`, building a processing Topology.
    *   Gotchas: SerDes mismatch, co-partitioning constraints.
7.  **[Lesson 7: Spring Cloud Stream](0007-spring-cloud-stream.md)**
    *   Destination Binders and functional programming model.
    *   Gotchas: Legacy annotations and error handling.
8.  **[Lesson 8: Spring Cloud Bus](0008-spring-cloud-bus.md)**
    *   Broadcasting control-plane management tasks and config refreshes.
    *   Gotchas: Using proper destinations and avoiding data transfer.
