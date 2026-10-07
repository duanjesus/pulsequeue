package com.pulsequeue.service;

import com.pulsequeue.entity.ProcessedEvent;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.metrics.EventMetrics;
import com.pulsequeue.notification.NotificationDeliveryException;
import com.pulsequeue.notification.NotificationDispatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventProcessingServiceTest {

    @Mock
    private ProcessedEventRecorder recorder;
    @Mock
    private NotificationDispatchService dispatchService;
    @Mock
    private DeduplicationService deduplicationService;
    @Mock
    private EventMetrics metrics;

    private EventProcessingService service;

    @BeforeEach
    void setUp() {
        service = new EventProcessingService(recorder, dispatchService, deduplicationService, metrics);
    }

    @Test
    void successfulProcessingRecordsProcessedAndClaimsDedupKey() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 10));
        ProcessedEvent record = ProcessedEvent.received(event.eventId(), event.eventType(), event.sourceService(), "{}",
                event.occurredAt());
        when(recorder.findOrCreate(event)).thenReturn(record);
        when(deduplicationService.isDuplicate(event.eventId())).thenReturn(false);

        service.process(event);

        verify(dispatchService).dispatch(event);
        verify(recorder).recordProcessed(record);
        verify(deduplicationService).markProcessed(event.eventId());
        verify(metrics).recordProcessed(any(Duration.class));
    }

    @Test
    void duplicateEventSkipsDispatchAndRecordsDuplicate() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 10));
        ProcessedEvent record = ProcessedEvent.received(event.eventId(), event.eventType(), event.sourceService(), "{}",
                event.occurredAt());
        when(recorder.findOrCreate(event)).thenReturn(record);
        when(deduplicationService.isDuplicate(event.eventId())).thenReturn(true);

        service.process(event);

        verify(dispatchService, never()).dispatch(any());
        verify(recorder).recordDuplicate(record);
        verify(metrics).recordDuplicate();
        verify(deduplicationService, never()).markProcessed(any());
    }

    @Test
    void failedDispatchRecordsFailureAndRethrows() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("simulateFailure", true));
        ProcessedEvent record = ProcessedEvent.received(event.eventId(), event.eventType(), event.sourceService(), "{}",
                event.occurredAt());
        when(recorder.findOrCreate(event)).thenReturn(record);
        when(deduplicationService.isDuplicate(event.eventId())).thenReturn(false);
        doThrow(new NotificationDeliveryException("boom")).when(dispatchService).dispatch(event);

        assertThrows(NotificationDeliveryException.class, () -> service.process(event));

        verify(recorder).recordFailed(record, "boom");
        verify(metrics).recordFailed();
        verify(deduplicationService, never()).markProcessed(any());
    }

    @Test
    void reprocessingAnAlreadyFailedRecordRecordsARetryMetric() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 10));
        ProcessedEvent existing = ProcessedEvent.received(event.eventId(), event.eventType(), event.sourceService(), "{}",
                event.occurredAt());
        existing.markFailed("previous attempt failed");
        when(recorder.findOrCreate(event)).thenReturn(existing);
        when(deduplicationService.isDuplicate(event.eventId())).thenReturn(false);

        service.process(event);

        verify(metrics).recordRetried();
        verify(recorder).recordProcessed(existing);
    }
}
