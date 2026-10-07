package com.pulsequeue.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.pulsequeue.entity.WebhookSubscription;

import java.time.Instant;

/** {@code secret} is only populated in the response to the request that created the subscription. */
public record WebhookResponse(
        Long id,
        String url,
        String eventTypePattern,
        boolean active,
        int consecutiveFailures,
        Instant createdAt,
        @JsonInclude(JsonInclude.Include.NON_NULL) String secret
) {

    public static WebhookResponse from(WebhookSubscription subscription) {
        return of(subscription, null);
    }

    public static WebhookResponse withSecret(WebhookSubscription subscription) {
        return of(subscription, subscription.getSecret());
    }

    private static WebhookResponse of(WebhookSubscription subscription, String secret) {
        return new WebhookResponse(subscription.getId(), subscription.getUrl(), subscription.getEventTypePattern(),
                subscription.isActive(), subscription.getConsecutiveFailures(), subscription.getCreatedAt(), secret);
    }
}
