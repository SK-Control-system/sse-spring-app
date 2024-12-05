package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RestController
public class SseController {

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
            throw new IllegalArgumentException("Video ID must not be null or blank");
        }

        // SSE Emitter 생성
        SseEmitter emitter = sseEmitters.addEmitter(videoId);

        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                // 1. Kafka에서 최신 100개 데이터 가져와 배열로 전송
                List<JsonNode> pastData = kafkaConsumerService.getLatestData(videoId);
                if (!pastData.isEmpty()) {
                    emitter.send(SseEmitter.event().name("message").data(pastData)); // 초기 데이터를 배열로 전송
                }

                // 2. Keep-Alive Ping 메시지 전송
                Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
                    try {
                        emitter.send(SseEmitter.event().name("ping").data("keep-alive"));
                    } catch (Exception e) {
                        emitter.completeWithError(e);
                    }
                }, 0, 30, TimeUnit.SECONDS); // 30초마다 "ping" 메시지 전송

                // 3. 실시간 데이터 스트리밍
                kafkaConsumerService.subscribe(videoId, data -> {
                    try {
                        emitter.send(SseEmitter.event().name("message").data(data.toString())); // 실시간 데이터를 개별 전송
                    } catch (Exception e) {
                        emitter.completeWithError(e);
                    }
                });

            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });

        // SSE 연결 종료 처리
        emitter.onCompletion(() -> sseEmitters.removeEmitter(videoId, emitter));
        emitter.onTimeout(() -> sseEmitters.removeEmitter(videoId, emitter));

        return emitter;
    }
}
