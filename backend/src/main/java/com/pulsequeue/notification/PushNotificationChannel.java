package com.pulsequeue.notification;

import com.pulsequeue.event.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Simulated channel — logs what a real push provider (FCM/APNs) integration
 * would send, rather than calling one.
 */
@Component
public class PushNotificationChannel implements NotificationChannel {

    private static final Logger log = LoggerFactory.getLogger(PushNotificationChannel.class);

    @Override
    public String getChannelName() {
        return "PUSH";
    }

    @Override
    public List<String> targetsFor(DomainEvent event) {
        return List.of("simulated");
    }

    @Override
    public void send(DomainEvent event, String target) {
        log.info("[Push] Notified about {} from {} (eventId={})",
                event.eventType(), event.sourceService(), event.eventId());
    }
}
