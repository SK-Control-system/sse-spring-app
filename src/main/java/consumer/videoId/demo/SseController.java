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

    // SLF4J Logger 추가
    private static final Logger logger = LoggerFactory.getLogger(SseController.class);

    private final KafkaConsumerService kafkaConsumerService;
    private final SseEmitters sseEmitters;

    // 생성자를 통해 KafkaConsumerService 및 SseEmitters 주입
    public SseController(KafkaConsumerService kafkaConsumerService, SseEmitters sseEmitters) {
        this.kafkaConsumerService = kafkaConsumerService;
        this.sseEmitters = sseEmitters;
    }

    /**
     * 클라이언트가 SSE 연결을 시작하면, 비디오 ID를 기반으로
     * 최신 100개의 데이터를 즉시 전송하고, 이후 실시간 스트리밍을 제공합니다.
     *
     * @param videoId 클라이언트가 요청한 비디오 ID
     * @return SseEmitter SSE 연결 객체
     */
    @CrossOrigin(origins = "*")
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam String videoId) {
        // 비디오 ID 유효성 검증
        if (videoId == null || videoId.isBlank()) {
            logger.error("Invalid videoId received: {}", videoId);
            throw new IllegalArgumentException("Video ID must not be null or blank");
        }

        logger.info("Starting SSE stream for videoId: {}", videoId);

        // SSE Emitter 생성
        SseEmitter emitter = sseEmitters.addEmitter(videoId);

        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                // 1. 최신 100개의 데이터 배열로 전송
                List<JsonNode> pastData = kafkaConsumerService.getLatestData(videoId);
                if (!pastData.isEmpty()) {
                    logger.info("Sending latest 100 data for videoId: {}", videoId);
                    emitter.send(SseEmitter.event().name("message").data(pastData));
                } else {
                    logger.info("No past data to send for videoId: {}", videoId);
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

        // SSE 연결 종료 처리
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
