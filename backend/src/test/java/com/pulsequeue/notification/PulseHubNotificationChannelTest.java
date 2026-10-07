package com.pulsequeue.notification;

import com.pulsequeue.config.PulseHubProperties;
import com.pulsequeue.entity.InstitutionMapping;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.repository.InstitutionMappingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class PulseHubNotificationChannelTest {

    private final DomainEvent surplusAlert = DomainEvent.of("supply.surplus_alert", "social-supply",
            Map.of("targetInstitutionId", 7, "message", "Sobra de arroz"));

    private InstitutionMappingRepository mappingRepository;
    private MockRestServiceServer server;
    private PulseHubNotificationChannel channel;

    @BeforeEach
    void setUp() {
        mappingRepository = mock(InstitutionMappingRepository.class);
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        PulseHubProperties properties = new PulseHubProperties("http://pulsehub", "test-key");
        channel = new PulseHubNotificationChannel(mappingRepository, properties, builder);
    }

    @Test
    void hasNoTargetWhenPayloadHasNoTargetInstitutionOrMessage() {
        DomainEvent event = DomainEvent.of("donation.created", "social-supply", Map.of("donationId", 1));

        assertTrue(channel.targetsFor(event).isEmpty());

        verifyNoInteractions(mappingRepository);
    }

    @Test
    void hasNoTargetWhenInstitutionHasNoPulseHubMapping() {
        when(mappingRepository.findByInstitutionId(7L)).thenReturn(Optional.empty());

        assertTrue(channel.targetsFor(surplusAlert).isEmpty());

        verify(mappingRepository).findByInstitutionId(7L);
    }

    @Test
    void targetsTheMappedPulseHubUser() {
        when(mappingRepository.findByInstitutionId(7L)).thenReturn(Optional.of(InstitutionMapping.of(7L, 42L)));

        assertEquals(List.of("42"), channel.targetsFor(surplusAlert));
    }

    @Test
    void deliversToPulseHub() {
        server.expect(requestTo("http://pulsehub/api/v1/system-messages"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(header("X-API-Key", "test-key"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json("{\"targetUserId\":42,\"content\":\"Sobra de arroz\"}"))
                .andRespond(withSuccess());

        channel.send(surplusAlert, "42");

        server.verify();
    }

    @Test
    void failsLoudlyButIsBestEffortSoTheDispatcherWillNotFailTheEvent() {
        server.expect(requestTo("http://pulsehub/api/v1/system-messages"))
                .andRespond(withServerError());

        assertThrows(RestClientException.class, () -> channel.send(surplusAlert, "42"));
        assertTrue(channel.isBestEffort());
    }
}
