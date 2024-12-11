package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class SseEmitters {

    private static final Logger logger = LoggerFactory.getLogger(SseEmitters.class);

    // 비디오 ID별로 Emitter 리스트를 관리
    private final ConcurrentHashMap<String, List<SseEmitter>> emitters = new ConcurrentHashMap<>();

    /**
     * 비디오 ID에 대한 Emitter를 추가하고 반환.
     */
    public SseEmitter addEmitter(String videoId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE); // 무한 타임아웃 설정

        // 비디오 ID에 해당하는 Emitter 리스트를 관리
        emitters.computeIfAbsent(videoId, key -> new CopyOnWriteArrayList<>()).add(emitter);

        // Emitter 이벤트 핸들러 설정
        emitter.onCompletion(() -> removeEmitter(videoId, emitter));
        emitter.onTimeout(() -> {
            logger.info("Emitter timed out for videoId: {}", videoId);
            removeEmitter(videoId, emitter);
        });
        emitter.onError(e -> {
            logger.warn("Emitter error for videoId: {}", videoId, e);
            removeEmitter(videoId, emitter);
        });

        return emitter;
    }

    /**
     * 비디오 ID에 해당하는 모든 Emitter에 이벤트 전송.
     */
    public void sendEvent(String videoId, JsonNode data) {
        List<SseEmitter> emitterList = emitters.get(videoId);
        if (emitterList != null) {
            for (SseEmitter emitter : emitterList) {
                try {
                    emitter.send(SseEmitter.event()
                            .name("message") // 이벤트 이름
                            .data(data));   // 데이터 전송
                } catch (IOException e) {
                    logger.warn("Failed to send event for videoId: {}. Removing emitter.", videoId, e);
                    removeEmitter(videoId, emitter); // 실패한 Emitter는 제거
                }
            }
        } else {
            logger.info("No emitters found for videoId: {}", videoId);
        }
    }

    /**
     * 특정 Emitter를 제거.
     */
    public void removeEmitter(String videoId, SseEmitter emitter) {
        List<SseEmitter> emitterList = emitters.get(videoId);
        if (emitterList != null) {
            emitterList.remove(emitter);
            logger.info("Removed emitter for videoId: {}. Remaining emitters: {}", videoId, emitterList.size());
            if (emitterList.isEmpty()) {
                emitters.remove(videoId); // 비디오 ID에 더 이상 Emitter가 없으면 삭제
                logger.info("No emitters left for videoId: {}. Removed from map.", videoId);
            }
        }
    }
}