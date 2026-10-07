package com.pulsequeue.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsequeue.entity.ProcessedEvent;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.producer.EventPublisher;
import com.pulsequeue.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Puts a dead-lettered event back on the main queue. The message itself is
 * gone from RabbitMQ by then (DeadLetterConsumer acked it), so the event is
 * rebuilt from its ProcessedEvent row with the <em>same</em> eventId: that is
 * what lets delivery tracking skip every destination that already succeeded
 * and retry only the ones that failed. Deduplication does not get in the way,
 * because its key is only written when an event succeeds.
 */
@Service
public class EventReplayService {

    public enum Result {
        REPLAYED,
        NOT_FOUND,
        NOT_DEAD_LETTERED
    }

    private static final Logger log = LoggerFactory.getLogger(EventReplayService.class);
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final ProcessedEventRepository repository;
    private final ProcessedEventRecorder recorder;
    private final EventPublisher publisher;
    private final ObjectMapper objectMapper;

    public EventReplayService(ProcessedEventRepository repository, ProcessedEventRecorder recorder,
                              EventPublisher publisher, ObjectMapper objectMapper) {
        this.repository = repository;
        this.recorder = recorder;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    public Result replay(String eventId) {
        Optional<ProcessedEvent> record = repository.findByEventId(eventId);
        if (record.isEmpty()) {
            return Result.NOT_FOUND;
        }
        // Rebuild before changing any state, so a row that cannot be rebuilt is left untouched.
        DomainEvent event = rebuild(record.get());

        // The claim is committed before publishing: the consumer must never read the row mid-change.
        if (!recorder.claimForReplay(eventId)) {
            return Result.NOT_DEAD_LETTERED;
        }
        try {
            publisher.publish(event);
        } catch (RuntimeException ex) {
            // Nothing reached the queue, so without this the row would sit in RECEIVED forever.
            recorder.releaseReplayClaim(eventId);
            throw ex;
        }
        log.info("Replayed dead-lettered eventId={} type={}", eventId, event.eventType());
        return Result.REPLAYED;
    }

    private DomainEvent rebuild(ProcessedEvent record) {
        Instant occurredAt = record.getOccurredAt() != null ? record.getOccurredAt() : record.getReceivedAt();
        try {
            return new DomainEvent(record.getEventId(), record.getEventType(), record.getSourceService(),
                    objectMapper.readValue(record.getPayload(), PAYLOAD_TYPE), occurredAt);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Stored payload of eventId=" + record.getEventId() + " is not valid JSON", ex);
        }
    }
}
