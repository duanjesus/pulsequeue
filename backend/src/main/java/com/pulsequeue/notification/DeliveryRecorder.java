package com.pulsequeue.notification;

import com.pulsequeue.entity.DeliveryStatus;
import com.pulsequeue.entity.NotificationDelivery;
import com.pulsequeue.repository.NotificationDeliveryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Each method is its own independently-committed transaction, for the same
 * reason as {@code ProcessedEventRecorder}: a failed dispatch ends with an
 * exception propagating to the retry interceptor, and the per-destination
 * outcomes written before it must survive that.
 */
@Service
public class DeliveryRecorder {

    private final NotificationDeliveryRepository repository;

    public DeliveryRecorder(NotificationDeliveryRepository repository) {
        this.repository = repository;
    }

    @Transactional(readOnly = true)
    public Set<DeliveryKey> findDelivered(String eventId) {
        return repository.findByEventIdAndStatus(eventId, DeliveryStatus.DELIVERED).stream()
                .map(delivery -> new DeliveryKey(delivery.getChannel(), delivery.getTarget()))
                .collect(Collectors.toSet());
    }

    @Transactional
    public void recordDelivered(String eventId, DeliveryKey key) {
        NotificationDelivery delivery = findOrCreate(eventId, key);
        delivery.markDelivered();
        repository.save(delivery);
    }

    @Transactional
    public void recordFailed(String eventId, DeliveryKey key, String error) {
        NotificationDelivery delivery = findOrCreate(eventId, key);
        delivery.markFailed(error);
        repository.save(delivery);
    }

    private NotificationDelivery findOrCreate(String eventId, DeliveryKey key) {
        return repository.findByEventIdAndChannelAndTarget(eventId, key.channel(), key.target())
                .orElseGet(() -> NotificationDelivery.pending(eventId, key.channel(), key.target()));
    }
}
