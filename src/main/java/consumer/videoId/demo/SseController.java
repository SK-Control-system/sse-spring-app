package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
public class SseController {

    private static final Logger logger = LoggerFactory.getLogger(SseController.class);
    private final KafkaConsumerService kafkaConsumerService;
    private final SseEmitters sseEmitters;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<SseEmitter, AtomicBoolean> initialDataSent = new ConcurrentHashMap<>();

    public SseController(KafkaConsumerService kafkaConsumerService, SseEmitters sseEmitters) {
        this.kafkaConsumerService = kafkaConsumerService;
        this.sseEmitters = sseEmitters;
    }

    @CrossOrigin(origins = "*")
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam String videoId) {
        if (videoId == null || videoId.isBlank()) {
            logger.error("Invalid videoId received: {}", videoId);
            throw new IllegalArgumentException("Video ID must not be null or blank");
        }

        logger.info("Starting SSE stream for videoId: {}", videoId);

        // SSE Emitter 생성
        SseEmitter emitter = sseEmitters.addEmitter(videoId);
        AtomicBoolean isInitialDataSent = new AtomicBoolean(false);
        initialDataSent.put(emitter, isInitialDataSent);

        executorService.execute(() -> {
            try {
                // 1. 최신 100개 데이터 배열 전송 (최초 1회만)
                if (!isInitialDataSent.get()) {
                    List<JsonNode> pastData = kafkaConsumerService.getLatestData(videoId);
                    if (!pastData.isEmpty()) {
                        logger.info("Sending latest 100 data for videoId: {}", videoId);
                        emitter.send(SseEmitter.event().name("initial").data(pastData));
                    } else {
                        logger.info("No past data to send for videoId: {}", videoId);
                    }
                    isInitialDataSent.set(true);
                }

                // 2. 실시간 데이터 전송 (이후부터는 실시간 데이터만 전송)
                kafkaConsumerService.subscribe(videoId, data -> {
                    try {
                        emitter.send(SseEmitter.event().name("message").data(data));
                    } catch (Exception e) {
                        logger.error("Error sending real-time data for videoId: {}", videoId, e);
                        emitter.completeWithError(e);
                    }
                });
            } catch (Exception e) {
                logger.error("Error in SSE stream for videoId: {}", videoId, e);
                emitter.completeWithError(e);
            }
        });

        // SSE 연결 종료 처리
        emitter.onCompletion(() -> {
            logger.info("SSE stream completed for videoId: {}", videoId);
            sseEmitters.removeEmitter(videoId, emitter);
            initialDataSent.remove(emitter);
        });
        emitter.onTimeout(() -> {
            logger.info("SSE stream timed out for videoId: {}", videoId);
            sseEmitters.removeEmitter(videoId, emitter);
            initialDataSent.remove(emitter);
        });

        return emitter;
    }
}
