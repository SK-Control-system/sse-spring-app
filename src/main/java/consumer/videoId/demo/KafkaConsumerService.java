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
import java.util.function.Consumer;

@Component
public class KafkaConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, Queue<JsonNode>> videoDataMap = new ConcurrentHashMap<>();
    private final SseEmitters sseEmitters;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerService(SseEmitters sseEmitters) {
        this.sseEmitters = sseEmitters;
    }

    @PostConstruct
    public void startKafkaConsumer() {
        int maxRetries = 5;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                initializeConsumer();
                break; // 연결 성공 시 루프 종료
            } catch (Exception e) {
                retryCount++;
                System.err.println("Kafka Consumer 재연결 시도: " + retryCount);
                try {
                    Thread.sleep(5000); // 재시도 간격
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (retryCount == maxRetries) {
            System.err.println("Kafka Consumer 재연결 실패");
            throw new RuntimeException("Kafka Consumer 재연결 실패");
        }
    }

    private void initializeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-svc:9093");
        props.put("group.id", "video-sse-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // 데이터 손실 방지를 위해 earliest로 설정

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("CHAT2"));

        // Kafka Polling
        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        processKafkaMessage(record.value());
                    }
                }
            } catch (WakeupException e) {
                System.out.println("Kafka consumer shutting down.");
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
            String videoId = rootNode.get("videoId").asText();
            JsonNode itemsNode = rootNode.get("items");

            if (itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    processItem(videoId, itemNode);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processItem(String videoId, JsonNode itemNode) {
        // 데이터 저장
        Queue<JsonNode> queue = videoDataMap.computeIfAbsent(videoId, key -> new ConcurrentLinkedQueue<>());

        synchronized (queue) {
            // 큐 크기를 100개로 제한하여 가장 오래된 데이터 삭제
            if (queue.size() >= 100) {
                queue.poll(); // 가장 오래된 데이터 제거
            }
            queue.offer(itemNode); // 새로운 데이터 추가
        }

        // SSE로 데이터 전송
        sseEmitters.sendEvent(videoId, itemNode);
    }

    // 최초 요청 시 최신 100개의 데이터 반환
    public List<JsonNode> getLatestData(String videoId) {
        Queue<JsonNode> queue = videoDataMap.get(videoId);
        if (queue == null) {
            return Collections.emptyList();
        }

        List<JsonNode> result = new ArrayList<>(queue);
        return result.subList(Math.max(result.size() - 100, 0), result.size());
    }

    public void subscribe(String videoId, Consumer<JsonNode> callback) {
        // videoId에 해당하는 데이터를 가져옴
        Queue<JsonNode> queue = videoDataMap.get(videoId);
        if (queue != null) {
            for (JsonNode data : queue) {
                callback.accept(data); // 데이터 처리 콜백 호출
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
