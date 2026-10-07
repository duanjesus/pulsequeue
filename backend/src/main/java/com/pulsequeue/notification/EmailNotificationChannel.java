package com.pulsequeue.notification;

import com.pulsequeue.event.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Simulated channel — logs what a real email provider integration would send,
 * rather than calling one. Mirrors the "Email" leg of the platform's fan-out.
 */
@Component
public class EmailNotificationChannel implements NotificationChannel {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationChannel.class);

    @Override
    public String getChannelName() {
        return "EMAIL";
    }

    @Override
    public List<String> targetsFor(DomainEvent event) {
        return List.of("simulated");
    }

    @Override
    public void send(DomainEvent event, String target) {
        log.info("[Email] Notified about {} from {} (eventId={})",
                event.eventType(), event.sourceService(), event.eventId());
    }
}
