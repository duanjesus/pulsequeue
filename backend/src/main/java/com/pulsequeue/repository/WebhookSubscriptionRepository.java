package com.pulsequeue.repository;

import com.pulsequeue.entity.WebhookSubscription;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WebhookSubscriptionRepository extends JpaRepository<WebhookSubscription, Long> {

    List<WebhookSubscription> findByActiveTrue();

    List<WebhookSubscription> findAllByOrderByIdAsc();
}
