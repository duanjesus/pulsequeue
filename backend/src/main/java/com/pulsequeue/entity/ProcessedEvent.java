package com.pulsequeue.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "processed_events")
public class ProcessedEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "source_service", nullable = false)
    private String sourceService;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProcessedEventStatus status;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "replay_count", nullable = false)
    private int replayCount;

    @Column(name = "occurred_at")
    private Instant occurredAt;

    protected ProcessedEvent() {
        // JPA
    }

    public static ProcessedEvent received(String eventId, String eventType, String sourceService, String payload,
                                          Instant occurredAt) {
        ProcessedEvent record = new ProcessedEvent();
        record.eventId = eventId;
        record.eventType = eventType;
        record.sourceService = sourceService;
        record.payload = payload;
        record.occurredAt = occurredAt;
        record.status = ProcessedEventStatus.RECEIVED;
        record.retryCount = 0;
        record.receivedAt = Instant.now();
        return record;
    }

    public void markProcessed() {
        this.status = ProcessedEventStatus.PROCESSED;
        this.processedAt = Instant.now();
        this.errorMessage = null;
    }

    public void markFailed(String errorMessage) {
        this.status = ProcessedEventStatus.FAILED;
        this.errorMessage = errorMessage;
        this.retryCount++;
    }

    public void markDeadLettered() {
        this.status = ProcessedEventStatus.DEAD_LETTERED;
        this.processedAt = Instant.now();
    }

    public void markDuplicate() {
        this.status = ProcessedEventStatus.DUPLICATE;
        this.processedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getSourceService() {
        return sourceService;
    }

    public ProcessedEventStatus getStatus() {
        return status;
    }

    public String getPayload() {
        return payload;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public int getReplayCount() {
        return replayCount;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}
