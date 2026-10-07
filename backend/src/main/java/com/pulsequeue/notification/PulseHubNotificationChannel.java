package com.pulsequeue.notification;

import com.pulsequeue.config.PulseHubProperties;
import com.pulsequeue.entity.InstitutionMapping;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.repository.InstitutionMappingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Bridges an event to a real PulseHub inbox: only applies when the payload
 * carries both {@code targetInstitutionId} and {@code message} and that
 * institution has a mapping — any other event has no target. The target is
 * the PulseHub user id. Best-effort: {@link #send} throws on failure like any
 * other channel, but {@link #isBestEffort()} tells the dispatcher to record
 * and log it rather than fail the event, so an unreachable PulseHub instance
 * can't turn an unrelated event into a retry/DLQ entry.
 */
@Component
public class PulseHubNotificationChannel implements NotificationChannel {

    private static final Logger log = LoggerFactory.getLogger(PulseHubNotificationChannel.class);

    private final InstitutionMappingRepository mappingRepository;
    private final RestClient restClient;
    private final String apiKey;

    public PulseHubNotificationChannel(InstitutionMappingRepository mappingRepository, PulseHubProperties properties,
                                        RestClient.Builder restClientBuilder) {
        this.mappingRepository = mappingRepository;
        this.restClient = restClientBuilder.baseUrl(properties.baseUrl()).build();
        this.apiKey = properties.apiKey();
    }

    @Override
    public String getChannelName() {
        return "PULSEHUB";
    }

    @Override
    public boolean isBestEffort() {
        return true;
    }

    @Override
    public List<String> targetsFor(DomainEvent event) {
        Object institutionIdRaw = event.payload().get("targetInstitutionId");
        if (institutionIdRaw == null || event.payload().get("message") == null) {
            return List.of();
        }

        Long institutionId = asLong(institutionIdRaw);
        if (institutionId == null) {
            log.warn("eventId={} has a non-numeric targetInstitutionId={}, skipping PulseHub delivery",
                    event.eventId(), institutionIdRaw);
            return List.of();
        }

        Optional<InstitutionMapping> mapping = mappingRepository.findByInstitutionId(institutionId);
        if (mapping.isEmpty()) {
            log.info("No PulseHub mapping for institutionId={}, skipping delivery for eventId={}",
                    institutionId, event.eventId());
            return List.of();
        }
        return List.of(String.valueOf(mapping.get().getPulsehubUserId()));
    }

    @Override
    public void send(DomainEvent event, String target) {
        restClient.post()
                .uri("/api/v1/system-messages")
                .header("X-API-Key", apiKey)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of(
                        "targetUserId", Long.parseLong(target),
                        "content", event.payload().get("message").toString()))
                .retrieve()
                .toBodilessEntity();
        log.info("Delivered eventId={} to PulseHub userId={}", event.eventId(), target);
    }

    private Long asLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String text) {
            try {
                return Long.parseLong(text);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        return null;
    }
}
