package com.pulsequeue.notification;

import com.pulsequeue.event.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Fans an event out to every (channel, target) that has not been delivered
 * yet. A retry or replay of the same event therefore resends only what
 * failed: one unreachable destination no longer causes the others to be
 * notified again. Every pending destination is attempted before the event is
 * failed, so one bad destination does not block the rest either.
 */
@Service
public class NotificationDispatchService {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatchService.class);

    private final List<NotificationChannel> channels;
    private final DeliveryRecorder deliveryRecorder;

    public NotificationDispatchService(List<NotificationChannel> channels, DeliveryRecorder deliveryRecorder) {
        this.channels = channels;
        this.deliveryRecorder = deliveryRecorder;
    }

    public void dispatch(DomainEvent event) {
        if (Boolean.TRUE.equals(event.payload().get("simulateFailure"))) {
            throw new NotificationDeliveryException(
                    "Simulated delivery failure for eventId=" + event.eventId());
        }

        Set<DeliveryKey> alreadyDelivered = deliveryRecorder.findDelivered(event.eventId());
        List<String> failures = new ArrayList<>();

        for (NotificationChannel channel : channels) {
            for (String target : channel.targetsFor(event)) {
                DeliveryKey key = new DeliveryKey(channel.getChannelName(), target);
                if (alreadyDelivered.contains(key)) {
                    log.debug("eventId={} already delivered via {} to {}, skipping",
                            event.eventId(), key.channel(), key.target());
                    continue;
                }
                try {
                    channel.send(event, target);
                } catch (RuntimeException ex) {
                    deliveryRecorder.recordFailed(event.eventId(), key, ex.getMessage());
                    if (channel.isBestEffort()) {
                        log.warn("Best-effort delivery of eventId={} via {} to {} failed: {}",
                                event.eventId(), key.channel(), key.target(), ex.getMessage());
                    } else {
                        failures.add(key.channel() + " -> " + key.target() + ": " + ex.getMessage());
                    }
                    continue;
                }
                deliveryRecorder.recordDelivered(event.eventId(), key);
            }
        }

        if (!failures.isEmpty()) {
            throw new NotificationDeliveryException(
                    "Delivery failed for eventId=" + event.eventId() + " [" + String.join("; ", failures) + "]");
        }
    }
}
