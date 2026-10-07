package com.pulsequeue.controller;

import com.pulsequeue.entity.WebhookSubscription;
import com.pulsequeue.security.ApiKeyAuthFilter;
import com.pulsequeue.security.ApiKeyProperties;
import com.pulsequeue.security.SecurityConfig;
import com.pulsequeue.service.WebhookSubscriptionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(WebhookController.class)
@Import(SecurityConfig.class)
@EnableConfigurationProperties(ApiKeyProperties.class)
@TestPropertySource(properties = "pulsequeue.security.api-key=test-secret-key")
class WebhookControllerSecurityTest {

    private static final String BODY = "{\"url\":\"http://receiver.test/hook\",\"eventTypePattern\":\"expense.*\"}";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private WebhookSubscriptionService service;

    @Test
    void creatingRequiresTheApiKey() throws Exception {
        mockMvc.perform(post("/api/v1/webhooks").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isUnauthorized());

        verifyNoInteractions(service);
    }

    @Test
    void deletingAndEnablingRequireTheApiKey() throws Exception {
        mockMvc.perform(delete("/api/v1/webhooks/1")).andExpect(status().isUnauthorized());
        mockMvc.perform(post("/api/v1/webhooks/1/enable")).andExpect(status().isUnauthorized());

        verifyNoInteractions(service);
    }

    @Test
    void creatingWithTheApiKeyReturnsTheSecretOnce() throws Exception {
        when(service.create("http://receiver.test/hook", "expense.*"))
                .thenReturn(WebhookSubscription.of("http://receiver.test/hook", "whsec_generated", "expense.*"));

        mockMvc.perform(post("/api/v1/webhooks")
                        .header(ApiKeyAuthFilter.API_KEY_HEADER, "test-secret-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(BODY))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.secret").value("whsec_generated"))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void listingIsOpenAndNeverExposesSecrets() throws Exception {
        when(service.findAll())
                .thenReturn(List.of(WebhookSubscription.of("http://receiver.test/hook", "whsec_generated", "*")));

        mockMvc.perform(get("/api/v1/webhooks"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].url").value("http://receiver.test/hook"))
                .andExpect(jsonPath("$[0].secret").doesNotExist());
    }

    @Test
    void anInvalidUrlIsABadRequestNotAServerError() throws Exception {
        when(service.create(any(), any())).thenThrow(new IllegalArgumentException("Webhook URL must be an absolute http or https URL"));

        mockMvc.perform(post("/api/v1/webhooks")
                        .header(ApiKeyAuthFilter.API_KEY_HEADER, "test-secret-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"url\":\"ftp://nope\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Webhook URL must be an absolute http or https URL"));
    }

    @Test
    void deletingAnUnknownSubscriptionIsNotFound() throws Exception {
        when(service.delete(99L)).thenReturn(false);

        mockMvc.perform(delete("/api/v1/webhooks/99").header(ApiKeyAuthFilter.API_KEY_HEADER, "test-secret-key"))
                .andExpect(status().isNotFound());
    }
}
