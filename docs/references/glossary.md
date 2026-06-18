# Kafka Learning Glossary

Canonical terminology used across this learning workspace. Concepts are compressed for quick lookup and revision.

## Key Kafka Concepts

**Event (Message)**:
A record of something that happened in the business domain. It consists of an optional key, value payload, timestamp, and optional headers.

**Broker**:
A single Kafka server responsible for receiving messages from producers, writing them to disk, and serving them to consumers.

**Topic**:
A logical feed or category of events. It is analogous to a table in a relational database but is append-only and immutable.

**Partition**:
A single commit log belonging to a Topic. It is the basic unit of scaling and parallelism in Kafka; records in a partition are strictly ordered by offset.

**Offset**:
A sequential, monotonic integer assigned to each record in a partition. It uniquely identifies the record's position.

**Consumer Group**:
A pool of consumers collaborating to read from a set of partitions. Kafka automatically ensures that each partition is assigned to exactly one consumer inside the group.

**Consumer Lag**:
The difference between the latest offset written by producers to a partition and the current offset read and committed by a consumer.

**KRaft (Kafka Raft)**:
The consensus protocol embedded directly within Kafka to manage metadata and coordinate leaders, replacing Apache ZooKeeper.

## Architectural Patterns

**Orchestration**:
A system coordination pattern where a central manager ("orchestrator") explicitly controls and commands the invocation flow of other services.

**Choreography**:
A coordination pattern where services act autonomously, listening to shared events and reacting to them independently without a central coordinator.
