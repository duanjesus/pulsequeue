package com.pulsequeue.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsequeue.entity.WebhookSubscription;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.service.WebhookSubscriptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.time.Instant;
import java.util.List;

/**
 * Pushes the event as signed JSON to every active subscription whose pattern
 * matches its type; each subscription id is one delivery target. Anything
 * other than a 2xx answer is a failed delivery, which fails the event (retry,
 * then DLQ) and counts towards disabling a subscription that keeps failing.
 */
@Component
public class WebhookNotificationChannel implements NotificationChannel {

    public static final String EVENT_ID_HEADER = "X-PulseQueue-Event-Id";
    public static final String EVENT_TYPE_HEADER = "X-PulseQueue-Event-Type";

    private static final Logger log = LoggerFactory.getLogger(WebhookNotificationChannel.class);

    private final WebhookSubscriptionService subscriptions;
    private final ObjectMapper objectMapper;
    private final RestClient restClient;

    public WebhookNotificationChannel(WebhookSubscriptionService subscriptions, ObjectMapper objectMapper,
                                      RestClient.Builder restClientBuilder) {
        this.subscriptions = subscriptions;
        this.objectMapper = objectMapper;
        this.restClient = restClientBuilder.build();
    }

    @Override
    public String getChannelName() {
        return "WEBHOOK";
    }

    @Override
    public List<String> targetsFor(DomainEvent event) {
        return subscriptions.findActiveMatching(event.eventType()).stream()
                .map(subscription -> String.valueOf(subscription.getId()))
                .toList();
    }

    @Override
    public void send(DomainEvent event, String target) {
        Long subscriptionId = Long.valueOf(target);
        WebhookSubscription subscription = subscriptions.findActive(subscriptionId)
                .orElseThrow(() -> new NotificationDeliveryException(
                        "Webhook subscription " + target + " is no longer active"));

        // The receiver verifies the signature against the bytes it gets, so sign exactly what is sent.
        byte[] body = serialize(event);
        long timestamp = Instant.now().getEpochSecond();

        try {
            ResponseEntity<Void> response = restClient.post()
                    .uri(URI.create(subscription.getUrl()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(WebhookSigner.SIGNATURE_HEADER, WebhookSigner.header(subscription.getSecret(), timestamp, body))
                    .header(EVENT_ID_HEADER, event.eventId())
                    .header(EVENT_TYPE_HEADER, event.eventType())
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new NotificationDeliveryException("Webhook answered HTTP " + response.getStatusCode().value());
            }
        } catch (RuntimeException ex) {
            subscriptions.recordFailure(subscriptionId);
            throw ex;
        }

        subscriptions.recordSuccess(subscriptionId);
        log.info("[Webhook] Delivered {} to subscription {} (eventId={})",
                event.eventType(), target, event.eventId());
    }

    private byte[] serialize(DomainEvent event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException ex) {
            throw new NotificationDeliveryException("Could not serialize eventId=" + event.eventId());
        }
    }
}
