package com.pulsequeue.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateWebhookRequest(
        @NotBlank @Size(max = 2048) String url,
        @Size(max = 100) String eventTypePattern
) {
}
