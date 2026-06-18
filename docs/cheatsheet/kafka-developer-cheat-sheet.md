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

---

## 2. TypeScript Implementation (`@platformatic/kafka`)

The `@platformatic/kafka` library provides clean, fast Kafka client support for TypeScript/Node.js environments.

### Producer Example
```typescript
import { Kafka } from '@platformatic/kafka'

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'order-service-ts'
})

const producer = kafka.producer()

async function publishOrder(orderId: string, payload: object) {
  await producer.connect()
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
}
```

### Consumer Example
```typescript
import { Kafka } from '@platformatic/kafka'

const kafka = new Kafka({
  brokers: ['localhost:9092'],
  clientId: 'order-processing-ts'
})

const consumer = kafka.consumer({ groupId: 'order-group-ts' })

async function startConsumer() {
  await consumer.connect()
  await consumer.subscribe({ topic: 'orders', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const key = message.key?.toString()
      const value = message.value?.toString()
      
      console.log(`Received message on partition ${partition}: Key=${key}, Value=${value}`)
      
      // Perform business actions here...
    }
  })
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
