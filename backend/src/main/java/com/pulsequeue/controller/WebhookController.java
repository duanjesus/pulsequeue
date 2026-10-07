package com.pulsequeue.controller;

import com.pulsequeue.dto.request.CreateWebhookRequest;
import com.pulsequeue.dto.response.WebhookResponse;
import com.pulsequeue.service.WebhookSubscriptionService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * Manages outbound webhook subscriptions. Reading the list is open like the
 * rest of the dashboard API (it never includes secrets); every write needs
 * the API key, enforced by ApiKeyAuthFilter.
 */
@RestController
@RequestMapping("/api/v1/webhooks")
public class WebhookController {

    private final WebhookSubscriptionService service;

    public WebhookController(WebhookSubscriptionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<WebhookResponse> create(@Valid @RequestBody CreateWebhookRequest request) {
        try {
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(WebhookResponse.withSecret(service.create(request.url(), request.eventTypePattern())));
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
        }
    }

    @GetMapping
    public List<WebhookResponse> list() {
        return service.findAll().stream().map(WebhookResponse::from).toList();
    }

    @PostMapping("/{id}/enable")
    public WebhookResponse enable(@PathVariable Long id) {
        return service.enable(id)
                .map(WebhookResponse::from)
                .orElseThrow(() -> notFound(id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        if (!service.delete(id)) {
            throw notFound(id);
        }
        return ResponseEntity.noContent().build();
    }

    private ResponseStatusException notFound(Long id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Webhook subscription " + id + " not found");
    }
}
