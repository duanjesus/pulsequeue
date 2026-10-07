package com.pulsequeue.notification;

import com.pulsequeue.event.DomainEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class NotificationDispatchServiceTest {

    private final DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 10));

    private DeliveryRecorder recorder;

    @BeforeEach
    void setUp() {
        recorder = mock(DeliveryRecorder.class);
        when(recorder.findDelivered(event.eventId())).thenReturn(Set.of());
    }

    @Test
    void deliversToEveryTargetOfEveryChannelAndRecordsEachOne() {
        NotificationChannel email = channel("EMAIL", "a@example.com");
        NotificationChannel webhook = channel("WEBHOOK", "1", "2");

        new NotificationDispatchService(List.of(email, webhook), recorder).dispatch(event);

        verify(email).send(event, "a@example.com");
        verify(webhook).send(event, "1");
        verify(webhook).send(event, "2");
        verify(recorder).recordDelivered(event.eventId(), new DeliveryKey("EMAIL", "a@example.com"));
        verify(recorder).recordDelivered(event.eventId(), new DeliveryKey("WEBHOOK", "1"));
        verify(recorder).recordDelivered(event.eventId(), new DeliveryKey("WEBHOOK", "2"));
    }

    @Test
    void skipsTargetsThatWereAlreadyDelivered() {
        NotificationChannel email = channel("EMAIL", "a@example.com");
        NotificationChannel webhook = channel("WEBHOOK", "1", "2");
        when(recorder.findDelivered(event.eventId())).thenReturn(Set.of(
                new DeliveryKey("EMAIL", "a@example.com"), new DeliveryKey("WEBHOOK", "1")));

        new NotificationDispatchService(List.of(email, webhook), recorder).dispatch(event);

        verify(email, never()).send(any(), anyString());
        verify(webhook, never()).send(event, "1");
        verify(webhook).send(event, "2");
    }

    @Test
    void failingTargetDoesNotBlockTheOthersButStillFailsTheEvent() {
        NotificationChannel webhook = channel("WEBHOOK", "1", "2");
        NotificationChannel email = channel("EMAIL", "a@example.com");
        doThrow(new IllegalStateException("503")).when(webhook).send(event, "1");

        NotificationDeliveryException thrown = assertThrows(NotificationDeliveryException.class,
                () -> new NotificationDispatchService(List.of(webhook, email), recorder).dispatch(event));

        assertTrue(thrown.getMessage().contains("WEBHOOK -> 1: 503"));
        verify(recorder).recordFailed(event.eventId(), new DeliveryKey("WEBHOOK", "1"), "503");
        verify(recorder, never()).recordDelivered(event.eventId(), new DeliveryKey("WEBHOOK", "1"));
        verify(recorder).recordDelivered(event.eventId(), new DeliveryKey("WEBHOOK", "2"));
        verify(recorder).recordDelivered(event.eventId(), new DeliveryKey("EMAIL", "a@example.com"));
    }

    @Test
    void bestEffortChannelFailureIsRecordedButDoesNotFailTheEvent() {
        NotificationChannel pulseHub = channel("PULSEHUB", "42");
        when(pulseHub.isBestEffort()).thenReturn(true);
        doThrow(new IllegalStateException("unreachable")).when(pulseHub).send(event, "42");

        new NotificationDispatchService(List.of(pulseHub), recorder).dispatch(event);

        verify(recorder).recordFailed(event.eventId(), new DeliveryKey("PULSEHUB", "42"), "unreachable");
    }

    @Test
    void channelWithNoTargetsIsNeverSent() {
        NotificationChannel pulseHub = channel("PULSEHUB");

        new NotificationDispatchService(List.of(pulseHub), recorder).dispatch(event);

        verify(pulseHub, never()).send(any(), anyString());
    }

    @Test
    void simulateFailureFlagThrowsWithoutCallingAnyChannel() {
        NotificationChannel email = channel("EMAIL", "a@example.com");
        DomainEvent failing = DomainEvent.of("expense.created", "cashpilot", Map.of("simulateFailure", true));

        assertThrows(NotificationDeliveryException.class,
                () -> new NotificationDispatchService(List.of(email), recorder).dispatch(failing));

        verify(email, never()).send(any(), anyString());
        verifyNoInteractions(recorder);
    }

    private NotificationChannel channel(String name, String... targets) {
        NotificationChannel channel = mock(NotificationChannel.class);
        when(channel.getChannelName()).thenReturn(name);
        when(channel.targetsFor(any())).thenReturn(List.of(targets));
        return channel;
    }
}
