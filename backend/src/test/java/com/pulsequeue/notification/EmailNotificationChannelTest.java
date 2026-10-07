package com.pulsequeue.notification;

import com.pulsequeue.config.EmailProperties;
import com.pulsequeue.event.DomainEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EmailNotificationChannelTest {

    private JavaMailSender mailSender;
    private EmailNotificationChannel channel;

    @BeforeEach
    void setUp() {
        mailSender = mock(JavaMailSender.class);
        channel = new EmailNotificationChannel(mailSender, new EmailProperties("notifications@pulsequeue.local"));
    }

    @Test
    void targetsTheRecipientInThePayload() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot",
                Map.of("recipientEmail", " finance@cashpilot.example "));

        assertEquals(List.of("finance@cashpilot.example"), channel.targetsFor(event));
    }

    @Test
    void hasNoTargetWithoutAUsableRecipient() {
        assertTrue(channel.targetsFor(DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 1))).isEmpty());
        assertTrue(channel.targetsFor(DomainEvent.of("expense.created", "cashpilot", Map.of("recipientEmail", " "))).isEmpty());
        assertTrue(channel.targetsFor(DomainEvent.of("expense.created", "cashpilot", Map.of("recipientEmail", 42))).isEmpty());
    }

    @Test
    void sendsAMessageDescribingTheEvent() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of(
                "recipientEmail", "finance@cashpilot.example", "message", "New expense", "amount", 149.9));

        channel.send(event, "finance@cashpilot.example");

        ArgumentCaptor<SimpleMailMessage> sent = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(sent.capture());
        SimpleMailMessage message = sent.getValue();
        assertEquals("notifications@pulsequeue.local", message.getFrom());
        assertArrayEquals(new String[]{"finance@cashpilot.example"}, message.getTo());
        assertEquals("[PulseQueue] expense.created from cashpilot", message.getSubject());
        assertTrue(message.getText().startsWith("New expense\n\n"));
        assertTrue(message.getText().contains("Event ID: " + event.eventId()));
        assertTrue(message.getText().contains("amount: 149.9"));
        assertFalse(message.getText().contains("recipientEmail"));
    }

    @Test
    void subjectCannotCarryLineBreaksFromTheProducer() {
        DomainEvent event = DomainEvent.of("expense.created\r\nBcc: someone@else.example", "cashpilot", Map.of());

        channel.send(event, "finance@cashpilot.example");

        ArgumentCaptor<SimpleMailMessage> sent = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(sent.capture());
        assertFalse(sent.getValue().getSubject().contains("\n"));
        assertFalse(sent.getValue().getSubject().contains("\r"));
    }

    @Test
    void smtpFailurePropagatesSoTheEventIsRetried() {
        doThrow(new MailSendException("connection refused")).when(mailSender).send(any(SimpleMailMessage.class));
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of());

        assertThrows(MailSendException.class, () -> channel.send(event, "finance@cashpilot.example"));
        assertFalse(channel.isBestEffort());
    }
}
