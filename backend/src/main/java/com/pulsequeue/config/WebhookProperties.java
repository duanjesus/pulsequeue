package com.pulsequeue.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pulsequeue.webhook")
public record WebhookProperties(
        int connectTimeoutMs,
        int readTimeoutMs,
        int maxConsecutiveFailures
) {
}
