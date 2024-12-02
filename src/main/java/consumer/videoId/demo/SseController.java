package consumer.videoId.demo;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.http.MediaType;

@RestController
public class SseController {

    private final SseEmitters sseEmitters;

    public SseController(SseEmitters sseEmitters) {
        this.sseEmitters = sseEmitters;
    }

    @CrossOrigin(origins = "*") // 모든 도메인에서 접근 허용
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> stream(@RequestParam String videoId) {
        System.out.println("Received SSE request for videoId: " + videoId);
        if (videoId == null || videoId.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(sseEmitters.addEmitter(videoId));
    }
}
