package com.pulsequeue.controller;

import com.pulsequeue.dto.request.PublishEventRequest;
import com.pulsequeue.dto.response.EventResponse;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.exception.RateLimitExceededException;
import com.pulsequeue.producer.EventPublisher;
import com.pulsequeue.service.RateLimitService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventPublisher publisher;
    private final RateLimitService rateLimitService;

    public EventController(EventPublisher publisher, RateLimitService rateLimitService) {
        this.publisher = publisher;
        this.rateLimitService = rateLimitService;
    }

    @PostMapping
    public ResponseEntity<EventResponse> publish(@Valid @RequestBody PublishEventRequest request) {
        if (!rateLimitService.tryAcquire(request.sourceService())) {
            throw new RateLimitExceededException(
                    "Rate limit exceeded for source service '" + request.sourceService() + "'");
        }
        DomainEvent event = DomainEvent.of(request.eventType(), request.sourceService(), request.payload());
        publisher.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new EventResponse(event.eventId(), "ACCEPTED"));
    }

    @PostMapping("/simulate")
    public ResponseEntity<List<EventResponse>> simulate() {
        List<DomainEvent> sample = List.of(
                DomainEvent.of("expense.created", "cashpilot", Map.of(
                        "expenseId", 4821,
                        "description", "Office supplies",
                        "amount", 149.90,
                        "recipientEmail", "finance@cashpilot.example")),
                DomainEvent.of("donation.created", "social-supply", Map.of(
                        "donationId", 1290,
                        "institution", "Abrigo Esperanca",
                        "itemCount", 12,
                        "recipientEmail", "donations@socialsupply.example"))
        );
        sample.forEach(publisher::publish);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(sample.stream().map(e -> new EventResponse(e.eventId(), "ACCEPTED")).toList());
    }
}
