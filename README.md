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
- [docs/mission.md](file:///Users/rohitkharche/testing/learning-kafka/docs/mission.md): Defines the core purpose and success outcomes of this learning journey.
- [docs/references/resources.md](file:///Users/rohitkharche/testing/learning-kafka/docs/references/resources.md): Curated list of high-trust external reading material and developer communities.
- [docs/index.md](file:///Users/rohitkharche/testing/learning-kafka/docs/index.md): Main documentation landing index page.
- [docs/lessons/](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/): Master Kafka systematically:
  1. [0001-kafka-basics-and-patterns.md](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0001-kafka-basics-and-patterns.md): Terminology and Event Choreography vs. Orchestration.
  2. [0002-setting-up-kafka.md](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0002-setting-up-kafka.md): KRaft Local Docker and Managed Cloud comparison.
  3. [0003-spring-boot-kafka.md](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0003-spring-boot-kafka.md): Spring Boot implementation.
  4. [0004-typescript-kafka.md](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0004-typescript-kafka.md): Platformatic TypeScript implementation.
  5. [0005-operations-and-troubleshooting.md](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0005-operations-and-troubleshooting.md): Operating and debugging.
- [docs/cheatsheet/](file:///Users/rohitkharche/testing/learning-kafka/docs/cheatsheet/): Developer quick references:
  - [kafka-developer-cheat-sheet.md](file:///Users/rohitkharche/testing/learning-kafka/docs/cheatsheet/kafka-developer-cheat-sheet.md): YAML settings, code templates, and common debugging commands.
- [docs/references/glossary.md](file:///Users/rohitkharche/testing/learning-kafka/docs/references/glossary.md): Terminology definitions.
- [learning-records/](file:///Users/rohitkharche/testing/learning-kafka/learning-records/): Summaries of your progress and updates to your Zone of Proximal Development (ZPD):
  - [0001-kafka-foundations.md](file:///Users/rohitkharche/testing/learning-kafka/learning-records/0001-kafka-foundations.md): Foundation and implementation track summaries.

## Getting Started

### 1. View Lessons
Read the Markdown lessons directly inside the `docs/lessons/` folder.

### 2. Run Kafka Locally
A `docker-compose.yml` config is detailed in [Lesson 2](file:///Users/rohitkharche/testing/learning-kafka/docs/lessons/0002-setting-up-kafka.md). To start your local Kafka instance:
```bash
docker compose up -d
```
Once up, open [Kafka UI](http://localhost:8080) to inspect clusters, messages, and consumer groups visually.
