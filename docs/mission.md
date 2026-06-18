---
icon: lucide/compass
---

# Mission: Apache Kafka Mastery

## Why
Master Apache Kafka to design, implement, and debug high-performance event-driven microservices. This will allow building resilient, scalable, and decoupled systems using industry-standard patterns, with concrete proficiency in both enterprise Java (Spring Boot) and modern TypeScript (`@platformatic/kafka`) environments.

## Success looks like
- **Core Understanding**: Explain Kafka's architecture (brokers, topics, partitions, consumer groups, offsets, and replication) with visual clarity.
- **Hands-on Implementations**: Write functional producers and consumers in both **Java Spring Boot** and **TypeScript** (using `@platformatic/kafka`).
- **Cluster Deployment**: Spin up a local multi-node Kafka cluster (via Docker Compose) and evaluate managed Kafka options (Confluent Cloud, AWS MSK, Upstash).
- **Architectural Fluency**: Model systems using both **Orchestration** and **Choreography** methodologies, understanding when to use each.
- **Troubleshooting Competence**: Identify, debug, and mitigate common production failure modes (consumer lag, serialization errors, rebalance storms, and message duplication).

## Constraints
- **Scope**: Focus on practical developer-centric usage, APIs, and troubleshooting, rather than low-level infrastructure sizing/OS kernel tuning.
- **Tooling**: Use Docker for local environments and standard cloud offerings for managed options.

## Out of scope
- Deep comparisons with non-event-stream platforms (e.g., RabbitMQ, ActiveMQ) beyond high-level architecture.
- Advanced infrastructure administration tasks (like writing custom partitioners or custom state store implementations in Kafka Streams) until core concepts are mastered.
