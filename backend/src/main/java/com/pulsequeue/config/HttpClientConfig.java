package com.pulsequeue.config;

import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class HttpClientConfig {

    /**
     * Outbound calls run on the consumer thread, so a destination that never
     * answers would stall the whole pipeline. This bounds every RestClient
     * built from the auto-configured builder (webhooks and the PulseHub bridge).
     */
    @Bean
    public RestClientCustomizer outboundTimeouts(WebhookProperties properties) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.DEFAULTS
                .withConnectTimeout(Duration.ofMillis(properties.connectTimeoutMs()))
                .withReadTimeout(Duration.ofMillis(properties.readTimeoutMs()));
        return builder -> builder.requestFactory(ClientHttpRequestFactories.get(settings));
    }
}
