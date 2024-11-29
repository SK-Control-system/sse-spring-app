package consumer.videoId.demo;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class SseController {

    private final SseEmitters sseEmitters;


    public SseController(SseEmitters sseEmitters) {
        this.sseEmitters = sseEmitters;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> stream(@RequestParam String videoId) {
        if (videoId == null || videoId.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(sseEmitters.addEmitter(videoId));
    }
}
