package com.pulsequeue.notification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pulsequeue.entity.WebhookSubscription;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.service.WebhookSubscriptionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class WebhookNotificationChannelTest {

    private static final String SECRET = "whsec_unit_test";
    private static final String URL = "http://receiver.test/hooks/pulsequeue";

    private final DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 10));
    private final WebhookSubscription subscription = WebhookSubscription.of(URL, SECRET, "*");

    private WebhookSubscriptionService subscriptions;
    private MockRestServiceServer server;
    private WebhookNotificationChannel channel;

    @BeforeEach
    void setUp() {
        subscriptions = mock(WebhookSubscriptionService.class);
        when(subscriptions.findActive(7L)).thenReturn(Optional.of(subscription));
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        channel = new WebhookNotificationChannel(
                subscriptions, new ObjectMapper().registerModule(new JavaTimeModule()), builder);
    }

    @Test
    void targetsEveryActiveSubscriptionMatchingTheEventType() {
        WebhookSubscription first = mock(WebhookSubscription.class);
        WebhookSubscription second = mock(WebhookSubscription.class);
        when(first.getId()).thenReturn(7L);
        when(second.getId()).thenReturn(9L);
        when(subscriptions.findActiveMatching("expense.created")).thenReturn(List.of(first, second));

        assertEquals(List.of("7", "9"), channel.targetsFor(event));
    }

    @Test
    void postsTheEventAsJsonSignedWithTheSubscriptionSecret() {
        server.expect(requestTo(URL))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(header(WebhookNotificationChannel.EVENT_ID_HEADER, event.eventId()))
                .andExpect(header(WebhookNotificationChannel.EVENT_TYPE_HEADER, "expense.created"))
                .andExpect(content().json("{\"eventId\":\"" + event.eventId() + "\",\"eventType\":\"expense.created\","
                        + "\"sourceService\":\"cashpilot\",\"payload\":{\"amount\":10}}"))
                .andExpect(request -> {
                    // Verify the way a receiver would: recompute the HMAC from the header timestamp and the raw body.
                    String signatureHeader = request.getHeaders().getFirst(WebhookSigner.SIGNATURE_HEADER);
                    assertTrue(signatureHeader.matches("t=\\d+,v1=[0-9a-f]{64}"), signatureHeader);
                    long timestamp = Long.parseLong(signatureHeader.substring(2, signatureHeader.indexOf(',')));
                    byte[] rawBody = ((MockClientHttpRequest) request).getBodyAsBytes();
                    assertEquals(WebhookSigner.header(SECRET, timestamp, rawBody), signatureHeader);
                })
                .andRespond(withSuccess());

        channel.send(event, "7");

        server.verify();
        verify(subscriptions).recordSuccess(7L);
        verify(subscriptions, never()).recordFailure(anyLong());
    }

    @Test
    void anErrorAnswerFailsTheDeliveryAndCountsAgainstTheSubscription() {
        server.expect(requestTo(URL)).andRespond(withStatus(HttpStatus.SERVICE_UNAVAILABLE));

        assertThrows(RestClientException.class, () -> channel.send(event, "7"));

        verify(subscriptions).recordFailure(7L);
        verify(subscriptions, never()).recordSuccess(anyLong());
        assertFalse(channel.isBestEffort());
    }

    @Test
    void aSubscriptionThatIsNoLongerActiveIsNotCalled() {
        when(subscriptions.findActive(7L)).thenReturn(Optional.empty());

        assertThrows(NotificationDeliveryException.class, () -> channel.send(event, "7"));

        server.verify();
    }
}
