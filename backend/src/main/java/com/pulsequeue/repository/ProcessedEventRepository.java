package com.pulsequeue.repository;

import com.pulsequeue.entity.ProcessedEvent;
import com.pulsequeue.entity.ProcessedEventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {

    Optional<ProcessedEvent> findByEventId(String eventId);

    long countByStatus(ProcessedEventStatus status);

    Page<ProcessedEvent> findAllByOrderByReceivedAtDesc(Pageable pageable);

    @Query("SELECT COALESCE(SUM(p.retryCount), 0) FROM ProcessedEvent p")
    long sumRetryCount();

    /**
     * Moves one event between statuses only if it is still in {@code from}, in a
     * single statement, so two concurrent replay requests cannot both win.
     * Returns the number of rows changed (0 or 1).
     */
    @Modifying
    @Query("UPDATE ProcessedEvent p SET p.status = :to, p.replayCount = p.replayCount + :replayDelta, "
            + "p.processedAt = :processedAt WHERE p.eventId = :eventId AND p.status = :from")
    int transition(@Param("eventId") String eventId,
                   @Param("from") ProcessedEventStatus from,
                   @Param("to") ProcessedEventStatus to,
                   @Param("replayDelta") int replayDelta,
                   @Param("processedAt") Instant processedAt);
}
