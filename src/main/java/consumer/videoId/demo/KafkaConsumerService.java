package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

@Component
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentMap<String, Queue<JsonNode>> videoDataMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> sentDataTracker = new ConcurrentHashMap<>();
    private final SseEmitters sseEmitters;
    private KafkaConsumer<String, String> consumer;
    private ExecutorService executorService;

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
                monitorQueueStates();
                break;
            } catch (Exception e) {
                retryCount++;
                logger.error("Kafka Consumer 재연결 시도: {}. Error: {}", retryCount, e.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (retryCount == maxRetries) {
            logger.error("Kafka Consumer 재연결 실패");
            throw new RuntimeException("Kafka Consumer 재연결 실패");
        }
    }

    private void initializeConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-svc:9093");
        props.put("group.id", "video-sse-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("CHAT2"));

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        processKafkaMessage(record.value());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Kafka consumer shutting down.");
            } catch (Exception e) {
                logger.error("Error in Kafka consumer thread", e);
            } finally {
                try {
                    consumer.close();
                    logger.info("Kafka consumer closed successfully.");
                } catch (Exception e) {
                    logger.error("Error while closing Kafka consumer", e);
                }
            }
        });
    }

    private void processKafkaMessage(String data) {
        try {
            JsonNode rootNode = objectMapper.readTree(data);
            String videoId = rootNode.get("videoId").asText();
            JsonNode itemsNode = rootNode.get("items");

            if (itemsNode.isArray()) {
                for (JsonNode itemNode : itemsNode) {
                    try {
                        processItem(videoId, itemNode);
                    } catch (Exception e) {
                        logger.error("Error processing item for videoId: {}", videoId, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Kafka message: {}", data, e);
        }
    }

    private void processItem(String videoId, JsonNode itemNode) {
        Queue<JsonNode> queue = videoDataMap.computeIfAbsent(videoId, key -> new ConcurrentLinkedQueue<>());
        Set<String> sentData = sentDataTracker.computeIfAbsent(videoId, key -> ConcurrentHashMap.newKeySet());

        String uniqueKey = itemNode.toString(); // 고유 키 생성
        synchronized (queue) {
            if (!sentData.contains(uniqueKey)) {
                if (queue.size() >= 100) {
                    queue.poll();
                }
                queue.offer(itemNode);
                sentData.add(uniqueKey); // 중복 데이터 추적
            }
        }

        logger.info("Added item to queue for videoId: {}. Queue size: {}", videoId, queue.size());
        sseEmitters.sendEvent(videoId, itemNode);
    }

    public List<JsonNode> getLatestData(String videoId) {
        Queue<JsonNode> queue = videoDataMap.get(videoId);
        if (queue == null) {
            return Collections.emptyList();
        }

        synchronized (queue) {
            List<JsonNode> result = new ArrayList<>(queue);
            return result.subList(Math.max(result.size() - 100, 0), result.size());
        }
    }

    public void subscribe(String videoId, java.util.function.Consumer<JsonNode> callback) {
        Queue<JsonNode> queue = videoDataMap.get(videoId);
        if (queue != null) {
            synchronized (queue) {
                queue.forEach(callback::accept);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        if (consumer != null) {
            consumer.wakeup();
        }

        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
    }

    private void monitorQueueStates() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> videoDataMap.forEach((videoId, queue) ->
                logger.info("VideoId: {}, Queue size: {}", videoId, queue.size())
        ), 0, 1, TimeUnit.MINUTES);
    }
}
