package consumer.videoId.demo;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.*;

@Component
public class SseEmitters {

    private final ConcurrentMap<String, CopyOnWriteArrayList<SseEmitter>> emitters = new ConcurrentHashMap<>();

    public SseEmitter addEmitter(String videoId) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);

        emitters.computeIfAbsent(videoId, key -> new CopyOnWriteArrayList<>()).add(emitter);

        emitter.onCompletion(() -> removeEmitter(videoId, emitter));
        emitter.onTimeout(() -> removeEmitter(videoId, emitter));
        emitter.onError(e -> removeEmitter(videoId, emitter));

        return emitter;
    }

    public void sendEvent(String videoId, JsonNode data) {
        CopyOnWriteArrayList<SseEmitter> emitterList = emitters.get(videoId);
        if (emitterList != null) {
            for (SseEmitter emitter : emitterList) {
                try {
                    emitter.send(SseEmitter.event().name("message").data(data));
                } catch (IOException e) {
                    System.err.println("Error sending event to SSE emitter: " + e.getMessage());
                    removeEmitter(videoId, emitter);
                }
            }
        }
    }

    private void removeEmitter(String videoId, SseEmitter emitter) {
        CopyOnWriteArrayList<SseEmitter> emitterList = emitters.get(videoId);
        if (emitterList != null) {
            emitterList.remove(emitter);
            if (emitterList.isEmpty()) {
                emitters.remove(videoId);
            }
        }
    }
}
