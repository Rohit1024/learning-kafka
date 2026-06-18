# Learning Record 0001: Kafka Foundations, Implementations & Operations

- **Date**: 2026-06-18
- **Lessons Completed**:
  - [Lesson 1: Kafka Basics & Architecture Patterns](../docs/lessons/0001-kafka-basics-and-patterns.md)
  - [Lesson 2: Setting up Kafka (Local Cluster & Cloud)](../docs/lessons/0002-setting-up-kafka.md)
  - [Lesson 3: Java Spring Boot Integration](../docs/lessons/0003-spring-boot-kafka.md)
  - [Lesson 4: TypeScript with @platformatic/kafka](../docs/lessons/0004-typescript-kafka.md)
  - [Lesson 5: Operations, Delivery Guarantees & Troubleshooting](../docs/lessons/0005-operations-and-troubleshooting.md)
- **Reference Materials Created**:
  - [Kafka Developer Reference Cheat Sheet](../docs/cheatsheet/kafka-developer-cheat-sheet.md)
  - [Kafka Learning Glossary](../docs/references/glossary.md)
- **Previous Knowledge Status**: Starting from the very basics of distributed event streaming.

## Key Insights
- **The Log Metaphor**: Kafka does not delete messages immediately after consumption; it works like an append-only commit ledger. Consumers track their progress independently via offsets, enabling replayability.
- **Partitions as Scaling Unit**: Splitting topics into partitions enables horizontal scalability because partitions can live on different brokers.
- **KRaft over ZooKeeper**: KRaft removes external cluster synchronization nodes, bringing coordination directly into Kafka and solving metadata out-of-sync bugs.
- **Interaction Models**: Event choreography provides the highest decoupling but requires distributed tracing to audit. Central orchestration provides high visibility but leads to a central bottleneck.
- **Language Implementations**:
  - *Java Spring Boot* makes use of declarative annotation-based consumption (`@KafkaListener`) and auto-injected configuration templates (`KafkaTemplate`).
  - *TypeScript (`@platformatic/kafka`)* handles non-blocking execution utilizing Node.js async-await loops for message handling.
- **Operations & Resiliency**:
  - *Manual Commits* (`enable-auto-commit: false`) prevent message loss in failure states.
  - *Deserialization Poison Pills* can be bypassed in Spring Boot using `ErrorHandlingDeserializer` delegating to DLQs.
  - *Rebalance Storms* can be mitigated by correctly setting `max.poll.interval.ms` relative to workload processing times.

## Zone of Proximal Development (ZPD) Update
- **Can do independently**:
  - Contrast event choreography vs. service orchestration.
  - Define Kafka brokers, topics, partitions, offsets, and consumer groups.
  - Set up a local single-node Kafka environment with KRaft via Docker Compose.
  - Implement basic asynchronous producers and manual-commit consumers in Java (Spring Boot) and TypeScript (`@platformatic/kafka`).
  - Diagnose consumer lag, rebalance loops, and poison pill crashes.
- **Next step (learning focus)**:
  - Setting up testing frameworks for Spring Kafka (using `@EmbeddedKafka` or Testcontainers) and TypeScript client testing.
  - Setting up real-time monitoring of broker metrics and schema management using Schema Registry.

## Open Questions / Revisions
- *None for this step. The foundation and development pathways have been successfully established.*
