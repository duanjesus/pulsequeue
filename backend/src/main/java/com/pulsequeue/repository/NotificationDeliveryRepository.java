package com.pulsequeue.repository;

import com.pulsequeue.entity.DeliveryStatus;
import com.pulsequeue.entity.NotificationDelivery;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface NotificationDeliveryRepository extends JpaRepository<NotificationDelivery, Long> {

    Optional<NotificationDelivery> findByEventIdAndChannelAndTarget(String eventId, String channel, String target);

    List<NotificationDelivery> findByEventIdAndStatus(String eventId, DeliveryStatus status);

    List<NotificationDelivery> findByEventIdOrderByIdAsc(String eventId);
}
