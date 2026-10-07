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

/**
 * One row per (event, channel, target): the outcome of delivering a single
 * event to a single destination. This is what lets a retry or replay resend
 * only the destinations that have not been delivered yet.
 */
@Entity
@Table(name = "notification_deliveries")
public class NotificationDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(nullable = false)
    private String channel;

    @Column(nullable = false)
    private String target;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected NotificationDelivery() {
        // JPA
    }

    public static NotificationDelivery pending(String eventId, String channel, String target) {
        NotificationDelivery delivery = new NotificationDelivery();
        delivery.eventId = eventId;
        delivery.channel = channel;
        delivery.target = target;
        delivery.status = DeliveryStatus.FAILED;
        delivery.attempts = 0;
        delivery.createdAt = Instant.now();
        delivery.updatedAt = delivery.createdAt;
        return delivery;
    }

    public void markDelivered() {
        this.status = DeliveryStatus.DELIVERED;
        this.lastError = null;
        this.attempts++;
        this.updatedAt = Instant.now();
    }

    public void markFailed(String error) {
        this.status = DeliveryStatus.FAILED;
        this.lastError = error;
        this.attempts++;
        this.updatedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getChannel() {
        return channel;
    }

    public String getTarget() {
        return target;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
