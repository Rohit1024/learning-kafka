# Apache Kafka Mastery: Learning Workspace

Welcome to your stateful Apache Kafka learning workspace. This repository is dedicated to teaching Kafka from the absolute basics to advanced enterprise development, featuring code implementations in **Java Spring Boot** and **TypeScript** (using `@platformatic/kafka`).

All course documentation and lessons are compiled in Markdown format under the `docs/` directory, compatible with standard static site generators.

## Learning Objectives
- **Fundamentals**: Topics, partitions, offsets, replication, and consumer groups.
- **Architectural Design**: Service Orchestration vs. Event Choreography.
- **Deployments**: Local Docker/KRaft clusters and Managed Cloud options.
- **Implementations**: Producer and Consumer services in Java and TypeScript.
- **Troubleshooting**: Lag checking, serialization issues, and rebalance storms.

## Workspace Layout
- [docs/mission.md](docs/mission.md): Defines the core purpose and success outcomes of this learning journey.
- [docs/references/resources.md](docs/references/resources.md): Curated list of high-trust external reading material and developer communities.
- [docs/index.md](docs/index.md): Main documentation landing index page.
- [docs/lessons/](docs/lessons/): Master Kafka systematically:
  1. **[Lesson 1: Kafka Basics & Architecture Patterns](docs/lessons/0001-kafka-basics-and-patterns.md)** — Learn terminology, event-streaming theory, and Service Orchestration vs. Event Choreography patterns.
  2. **[Lesson 2: Setting up Kafka (Local Cluster & Cloud)](docs/lessons/0002-setting-up-kafka.md)** — Set up a local Kafka cluster using Docker Compose with KRaft mode and compare managed cloud offerings.
  3. **[Lesson 3: Java Spring Boot Integration](docs/lessons/0003-spring-boot-kafka.md)** — Configure Spring Boot, implement asynchronous producers with `KafkaTemplate`, and build consumers with manual offset acknowledgments.
  4. **[Lesson 4: TypeScript with @platformatic/kafka](docs/lessons/0004-typescript-kafka.md)** — Implement producer and consumer services in Node.js TypeScript with `@platformatic/kafka` using asynchronous loops.
  5. **[Lesson 5: Operations, Delivery Guarantees & Troubleshooting](docs/lessons/0005-operations-and-troubleshooting.md)** — Manage consumer lag, prevent rebalance storms, and handle poison pills with Dead Letter Queues (DLQ).
- [docs/cheatsheet/](docs/cheatsheet/): Developer quick references:
  - [kafka-developer-cheat-sheet.md](docs/cheatsheet/kafka-developer-cheat-sheet.md): YAML settings, code templates, and common debugging commands.
- [docs/references/glossary.md](docs/references/glossary.md): Terminology definitions.
- [learning-records/](learning-records/): Summaries of your progress and updates to your Zone of Proximal Development (ZPD):
  - [0001-kafka-foundations.md](learning-records/0001-kafka-foundations.md): Foundation and implementation track summaries.

## Getting Started

### 1. View Lessons
Read the Markdown lessons directly inside the `docs/lessons/` folder.

### 2. Run Kafka Locally
A `docker-compose.yml` config is detailed in [Lesson 2](docs/lessons/0002-setting-up-kafka.md). To start your local Kafka instance:
```bash
docker compose up -d
```
Once up, open [Kafka UI](http://localhost:8080) to inspect clusters, messages, and consumer groups visually.
