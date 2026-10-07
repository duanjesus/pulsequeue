package com.pulsequeue.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * An HTTP endpoint that wants events pushed to it. {@code secret} signs every
 * delivery (HMAC needs the raw value, so it cannot be stored hashed) and is
 * only ever returned to the caller once, at creation.
 */
@Entity
@Table(name = "webhook_subscriptions")
public class WebhookSubscription {

    public static final String MATCH_ALL = "*";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 2048)
    private String url;

    @Column(nullable = false, length = 128)
    private String secret;

    @Column(name = "event_type_pattern", nullable = false, length = 100)
    private String eventTypePattern;

    @Column(nullable = false)
    private boolean active;

    @Column(name = "consecutive_failures", nullable = false)
    private int consecutiveFailures;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected WebhookSubscription() {
        // JPA
    }

    public static WebhookSubscription of(String url, String secret, String eventTypePattern) {
        WebhookSubscription subscription = new WebhookSubscription();
        subscription.url = url;
        subscription.secret = secret;
        subscription.eventTypePattern = eventTypePattern;
        subscription.active = true;
        subscription.consecutiveFailures = 0;
        subscription.createdAt = Instant.now();
        return subscription;
    }

    /** {@code *} matches everything, {@code expense.*} matches by prefix, anything else must be equal. */
    public boolean matches(String eventType) {
        if (eventTypePattern.endsWith(MATCH_ALL)) {
            return eventType.startsWith(eventTypePattern.substring(0, eventTypePattern.length() - 1));
        }
        return eventTypePattern.equals(eventType);
    }

    public void registerSuccess() {
        this.consecutiveFailures = 0;
    }

    /** Deactivates the subscription once it has failed {@code maxConsecutiveFailures} times in a row. */
    public void registerFailure(int maxConsecutiveFailures) {
        this.consecutiveFailures++;
        if (this.consecutiveFailures >= maxConsecutiveFailures) {
            this.active = false;
        }
    }

    public void enable() {
        this.active = true;
        this.consecutiveFailures = 0;
    }

    public Long getId() {
        return id;
    }

    public String getUrl() {
        return url;
    }

    public String getSecret() {
        return secret;
    }

    public String getEventTypePattern() {
        return eventTypePattern;
    }

    public boolean isActive() {
        return active;
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
