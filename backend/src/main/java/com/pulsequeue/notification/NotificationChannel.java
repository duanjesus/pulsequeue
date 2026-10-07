package com.pulsequeue.notification;

import com.pulsequeue.event.DomainEvent;

import java.util.List;

public interface NotificationChannel {

    String getChannelName();

    /**
     * The destinations this channel would deliver the event to. Empty means
     * the channel does not apply to this event. Each target is tracked and
     * retried independently, so it must be stable across calls for the same
     * event.
     */
    List<String> targetsFor(DomainEvent event);

    /** Delivers to one target; throws if the delivery did not happen. */
    void send(DomainEvent event, String target);

    /** A best-effort channel's failures are recorded but never fail the event. */
    default boolean isBestEffort() {
        return false;
    }
}
