# Kafka Developer Reference Cheat Sheet

Quick reference syntax and troubleshooting tips for Java Spring Boot and TypeScript (@platformatic/kafka).

## 1. Java Spring Boot Implementation

### Application Config (`application.yml`)
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all # Guarantee durability
    consumer:
      group-id: order-processing-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.dto"
      enable-auto-commit: false # Prefer manual offset commits for safety
      auto-offset-reset: earliest # Read from start if group has no offset
    listener:
      ack-mode: manual_immediate
```

### Producer Implementation
```java
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderCreated(String orderId, OrderEvent event) {
        // Sending with key (orderId) guarantees all events for this order go to the SAME partition
        this.kafkaTemplate.send("orders", orderId, event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    System.err.println("Failed to send message: " + ex.getMessage());
                } else {
                    System.out.println("Message sent successfully to partition: " + 
                                       result.getRecordMetadata().partition());
                }
            });
    }
}
```

### Consumer Implementation with Manual Acknowledgment
```java
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consume(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        try {
            OrderEvent order = record.value();
            System.out.printf("Processing Order ID: %s, Details: %s%n", record.key(), order);
            
            // Perform business logic here
            
            ack.acknowledge(); // Manually commit offset only after successful processing
        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
            // Optionally, do not acknowledge to trigger redelivery or send to Dead Letter Queue (DLQ)
        }
    }
}
```

### Java Serialization & Deserialization with Jackson 3
Jackson 3 relies on the `tools.jackson.databind.json.JsonMapper` package (instead of `com.fasterxml.jackson.databind.ObjectMapper`) and features unchecked runtime exceptions.

#### Manual Serialization & Deserialization
```java
import tools.jackson.databind.json.JsonMapper;

public class Jackson3Helper {
    private static final JsonMapper mapper = JsonMapper.builder().build();

    public static byte[] serialize(Object value) {
        return mapper.writeValueAsBytes(value); // throws unchecked RuntimeException
    }

    public static <T> T deserialize(byte[] src, Class<T> valueType) {
        return mapper.readValue(src, valueType); // throws unchecked RuntimeException
    }
}
```

#### Custom Kafka Serializer & Deserializer
```java
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.json.JsonMapper;
import java.util.Map;

// Custom Serializer
public class Jackson3Serializer<T> implements Serializer<T> {
    private final JsonMapper mapper = JsonMapper.builder().build();

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}

    @Override
    public byte[] serialize(String topic, T data) {
        return data == null ? null : mapper.writeValueAsBytes(data);
    }
}

// Custom Deserializer
public class Jackson3Deserializer<T> implements Deserializer<T> {
    private final JsonMapper mapper = JsonMapper.builder().build();
    private Class<T> targetType;

    public Jackson3Deserializer() {}
    public Jackson3Deserializer(Class<T> targetType) { this.targetType = targetType; }

    @Override public void close() {}
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeConfig = isKey ? "key.deserializer.type" : "value.deserializer.type";
        if (configs.containsKey(typeConfig)) {
            try {
                this.targetType = (Class<T>) Class.forName((String) configs.get(typeConfig));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) return null;
        if (targetType == null) throw new IllegalStateException("Target type class is not configured.");
        return mapper.readValue(data, targetType);
    }
}
```

---

## 2. TypeScript Implementation (`@platformatic/kafka`)

The `@platformatic/kafka` library provides clean, fast Kafka client support for TypeScript/Node.js environments.

### Producer Example
```typescript
import { Producer, stringSerializers } from '@platformatic/kafka'

const producer = new Producer({
  clientId: 'order-service-ts',
  bootstrapBrokers: ['localhost:9092'],
  serializers: stringSerializers
})

async function publishOrder(orderId: string, payload: object) {
  try {
    await producer.send({
      topic: 'orders',
      messages: [
        {
          key: orderId,
          value: JSON.stringify(payload)
        }
      ]
    })
    console.log(`Published order: ${orderId}`)
  } finally {
    // Close to release resources
    await producer.close()
  }
}
```

### Consumer Example
```typescript
import { Consumer, stringDeserializers } from '@platformatic/kafka'
import { forEach } from 'hwp'

const consumer = new Consumer({
  groupId: 'order-group-ts',
  clientId: 'order-processing-ts',
  bootstrapBrokers: ['localhost:9092'],
  deserializers: stringDeserializers
})

async function startConsumer() {
  const stream = await consumer.consume({
    autocommit: true,
    topics: ['orders'],
    sessionTimeout: 10000,
    heartbeatInterval: 500
  })

  // Option 1: Event-based consumption (push model)
  // stream.on('data', message => {
  //   console.log(`Received: ${message.key} -> ${message.value}`)
  // })

  // Option 2: Async iterator consumption (sequential pull model)
  // for await (const message of stream) {
  //   console.log(`Received: ${message.key} -> ${message.value}`)
  // }

  // Option 3: Concurrent processing (concurrency level 16)
  try {
    await forEach(
      stream,
      async message => {
        console.log(`Received: ${message.key} -> ${message.value}`)
        // Process message...
      },
      16
    )
  } finally {
    await consumer.close()
  }
}

startConsumer().catch(console.error)
```

---

## 3. Troubleshooting Guide & Common Failure Modes

### Issue 1: Deserialization Poison Pill
*   **Symptoms**: The consumer gets stuck in an infinite loop throwing serialization errors and cannot progress.
*   **Reason**: A producer wrote a message to the topic in a format the consumer cannot decode. The consumer reads it, fails, retries, reads it again, and stays stuck.
*   **Mitigation**: Implement an `ErrorHandlingDeserializer` in Spring Boot or route invalid messages to a Dead Letter Queue (DLQ) programmatically rather than letting the consumer crash repeatedly.

### Issue 2: CommitFailedException & Rebalance Storms
*   **Symptoms**: Consumer logs show `CommitFailedException: Commit cannot be completed since the group has already rebalanced`.
*   **Reason**: The consumer took longer than `max.poll.interval.ms` to process a batch of records. Kafka assumes the consumer has hung or died, kicks it out of the group, and assigns its partitions to other consumers, causing processing cascades.
*   **Mitigation**: Scale up `max.poll.interval.ms` config, or decrease the max batch size via `max.poll.records` so the consumer processes fewer records per poll loop.

### Monitoring Consumer Lag
Consumer Lag is the delta between the latest message offset in a partition and the offset committed by a consumer. Run the following CLI command to check lag:
```bash
docker exec -it kafka-local kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group order-processing-group
```
