package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Component
public class KafkaConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, BlockingQueue<JsonNode>> videoDataMap = new ConcurrentHashMap<>();
    private final SseEmitters sseEmitters;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerService(SseEmitters sseEmitters) {
        this.sseEmitters = sseEmitters;
    }

    @PostConstruct
    public void startKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-svc:9093");
        props.put("group.id", "video-sse-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("CHAT2"));

        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        processKafkaMessage(record.value());
                    }
                }
            } catch (WakeupException e) {
                System.out.println("Kafka consumer is shutting down.");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }).start();
    }

    private void processKafkaMessage(String data) {
        try {
            JsonNode rootNode = objectMapper.readTree(data);
            System.out.println("Received Kafka message: " + rootNode);

            String videoId = Optional.ofNullable(rootNode.get("videoId"))
                    .map(JsonNode::asText)
                    .orElseThrow(() -> new IllegalArgumentException("Missing 'videoId'"));

            JsonNode itemsNode = Optional.ofNullable(rootNode.get("items"))
                    .orElseThrow(() -> new IllegalArgumentException("Missing 'items'"));

            // Process each item in the 'items' array
            if (itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    processItem(videoId, itemNode);
                }
            } else {
                throw new IllegalArgumentException("'items' should be an array.");
            }
        } catch (Exception e) {
            System.err.println("Failed to process Kafka message: " + data);
            e.printStackTrace();
        }
    }

    private void processItem(String videoId, JsonNode itemNode) {
        try {
            videoDataMap.computeIfAbsent(videoId, key -> new LinkedBlockingQueue<>(100));
            BlockingQueue<JsonNode> queue = videoDataMap.get(videoId);

            synchronized (queue) {
                if (queue.size() >= 100) {
                    queue.poll();
                }
                queue.offer(itemNode);
            }

            // Send SSE event with the item data
            sseEmitters.sendEvent(videoId, itemNode);
        } catch (Exception e) {
            System.err.println("Failed to process item: " + itemNode);
            e.printStackTrace();
        }
    }

    public List<JsonNode> getVideoData(String videoId) {
        BlockingQueue<JsonNode> queue = videoDataMap.get(videoId);
        if (queue == null) {
            return Collections.emptyList();
        }
        synchronized (queue) {
            return new ArrayList<>(queue);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
