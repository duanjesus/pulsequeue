package com.pulsequeue.notification;

import com.pulsequeue.config.EmailProperties;
import com.pulsequeue.event.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Sends a real email over SMTP to the address in the payload's
 * {@code recipientEmail}; an event without one has no email target. Unlike
 * the PulseHub bridge this is not best-effort: an SMTP failure propagates, so
 * the event is retried and eventually dead-lettered.
 */
@Component
public class EmailNotificationChannel implements NotificationChannel {

    static final String RECIPIENT_KEY = "recipientEmail";

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationChannel.class);

    private final JavaMailSender mailSender;
    private final String from;

    public EmailNotificationChannel(JavaMailSender mailSender, EmailProperties properties) {
        this.mailSender = mailSender;
        this.from = properties.from();
    }

    @Override
    public String getChannelName() {
        return "EMAIL";
    }

    @Override
    public List<String> targetsFor(DomainEvent event) {
        Object recipient = event.payload().get(RECIPIENT_KEY);
        if (recipient instanceof String address && !address.isBlank()) {
            return List.of(address.trim());
        }
        return List.of();
    }

    @Override
    public void send(DomainEvent event, String target) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(from);
        message.setTo(target);
        // eventType and sourceService come from the producer; keep them from adding mail headers.
        message.setSubject(("[PulseQueue] " + event.eventType() + " from " + event.sourceService())
                .replaceAll("[\\r\\n]+", " "));
        message.setText(body(event));
        mailSender.send(message);
        log.info("[Email] Sent {} to {} (eventId={})", event.eventType(), target, event.eventId());
    }

    private String body(DomainEvent event) {
        StringBuilder text = new StringBuilder();
        Object message = event.payload().get("message");
        if (message != null) {
            text.append(message).append("\n\n");
        }
        text.append("Event: ").append(event.eventType()).append('\n');
        text.append("Source: ").append(event.sourceService()).append('\n');
        text.append("Event ID: ").append(event.eventId()).append('\n');
        text.append("Occurred at: ").append(event.occurredAt()).append('\n');
        for (Map.Entry<String, Object> entry : event.payload().entrySet()) {
            if (!RECIPIENT_KEY.equals(entry.getKey()) && !"message".equals(entry.getKey())) {
                text.append(entry.getKey()).append(": ").append(entry.getValue()).append('\n');
            }
        }
        return text.toString();
    }
}
