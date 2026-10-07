package com.pulsequeue.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pulsequeue.entity.ProcessedEvent;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.producer.EventPublisher;
import com.pulsequeue.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.amqp.AmqpConnectException;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class EventReplayServiceTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-10-01T12:00:00Z");
    private static final String PAYLOAD = "{\"amount\":149.9,\"recipientEmail\":\"finance@cashpilot.example\"}";

    private ProcessedEventRepository repository;
    private ProcessedEventRecorder recorder;
    private EventPublisher publisher;
    private EventReplayService service;

    @BeforeEach
    void setUp() {
        repository = mock(ProcessedEventRepository.class);
        recorder = mock(ProcessedEventRecorder.class);
        publisher = mock(EventPublisher.class);
        service = new EventReplayService(repository, recorder, publisher, new ObjectMapper());
    }

    @Test
    void republishesTheSameEventAfterClaimingIt() {
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(
                ProcessedEvent.received("e-1", "expense.created", "cashpilot", PAYLOAD, OCCURRED_AT)));
        when(recorder.claimForReplay("e-1")).thenReturn(true);

        assertEquals(EventReplayService.Result.REPLAYED, service.replay("e-1"));

        ArgumentCaptor<DomainEvent> published = ArgumentCaptor.forClass(DomainEvent.class);
        InOrder order = inOrder(recorder, publisher);
        order.verify(recorder).claimForReplay("e-1");
        order.verify(publisher).publish(published.capture());
        assertEquals(new DomainEvent("e-1", "expense.created", "cashpilot",
                Map.of("amount", 149.9, "recipientEmail", "finance@cashpilot.example"), OCCURRED_AT), published.getValue());
    }

    @Test
    void unknownEventIsNotFound() {
        when(repository.findByEventId("missing")).thenReturn(Optional.empty());

        assertEquals(EventReplayService.Result.NOT_FOUND, service.replay("missing"));

        verifyNoInteractions(recorder, publisher);
    }

    @Test
    void eventThatIsNotDeadLetteredIsNotPublished() {
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(
                ProcessedEvent.received("e-1", "expense.created", "cashpilot", PAYLOAD, OCCURRED_AT)));
        when(recorder.claimForReplay("e-1")).thenReturn(false);

        assertEquals(EventReplayService.Result.NOT_DEAD_LETTERED, service.replay("e-1"));

        verify(publisher, never()).publish(any());
    }

    @Test
    void brokerFailureGivesTheEventBackToTheDeadLetterState() {
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(
                ProcessedEvent.received("e-1", "expense.created", "cashpilot", PAYLOAD, OCCURRED_AT)));
        when(recorder.claimForReplay("e-1")).thenReturn(true);
        doThrow(new AmqpConnectException(new RuntimeException("connection refused"))).when(publisher).publish(any());

        assertThrows(AmqpConnectException.class, () -> service.replay("e-1"));

        verify(recorder).releaseReplayClaim("e-1");
    }

    @Test
    void rowRecordedBeforeOccurredAtExistedFallsBackToWhenItWasReceived() {
        ProcessedEvent legacy = ProcessedEvent.received("e-1", "expense.created", "cashpilot", PAYLOAD, null);
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(legacy));
        when(recorder.claimForReplay("e-1")).thenReturn(true);

        service.replay("e-1");

        ArgumentCaptor<DomainEvent> published = ArgumentCaptor.forClass(DomainEvent.class);
        verify(publisher).publish(published.capture());
        assertEquals(legacy.getReceivedAt(), published.getValue().occurredAt());
    }

    @Test
    void unreadableStoredPayloadLeavesTheRowUntouched() {
        when(repository.findByEventId("e-1")).thenReturn(Optional.of(
                ProcessedEvent.received("e-1", "expense.created", "cashpilot", "{not json", OCCURRED_AT)));

        assertThrows(IllegalStateException.class, () -> service.replay("e-1"));

        verifyNoInteractions(recorder, publisher);
    }
}
