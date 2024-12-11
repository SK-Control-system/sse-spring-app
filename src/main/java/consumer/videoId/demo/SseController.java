package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.concurrent.Executors;

@RestController
public class SseController {

    private static final Logger logger = LoggerFactory.getLogger(SseController.class);

    private final KafkaConsumerService kafkaConsumerService;
    private final SseEmitters sseEmitters;

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

        SseEmitter emitter = sseEmitters.addEmitter(videoId);

        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                // 1. 이전 데이터(최신 100개)를 한 번에 전송
                List<JsonNode> pastData = kafkaConsumerService.getLatestData(videoId);
                if (!pastData.isEmpty()) {
                    logger.info("Sending latest 100 data for videoId: {}", videoId);
                    emitter.send(SseEmitter.event().name("initial").data(pastData));
                }

                // 2. 실시간 데이터 스트리밍
                kafkaConsumerService.subscribe(videoId, data -> {
                    try {
                        logger.info("Sending real-time data for videoId: {}", videoId);
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

        emitter.onCompletion(() -> {
            logger.info("SSE stream completed for videoId: {}", videoId);
            sseEmitters.removeEmitter(videoId, emitter);
        });

        emitter.onTimeout(() -> {
            logger.info("SSE stream timed out for videoId: {}", videoId);
            sseEmitters.removeEmitter(videoId, emitter);
        });

        return emitter;
    }
}
