package com.pulsequeue.notification;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class WebhookSignerTest {

    private static final byte[] BODY =
            "{\"eventId\":\"e-1\",\"eventType\":\"expense.created\"}".getBytes(StandardCharsets.UTF_8);

    /** Expected value computed independently with Python's hmac/hashlib. */
    @Test
    void matchesAnIndependentlyComputedHmacSha256() {
        assertEquals("c1ba3a32eefd90ef059483ab4c9cd75bb666af06988deb2ed1695b7114a7c66f",
                WebhookSigner.signature("whsec_test_secret", 1700000000L, BODY));
    }

    @Test
    void headerCarriesTimestampAndSignature() {
        assertEquals("t=1700000000,v1=c1ba3a32eefd90ef059483ab4c9cd75bb666af06988deb2ed1695b7114a7c66f",
                WebhookSigner.header("whsec_test_secret", 1700000000L, BODY));
    }

    @Test
    void changesWithTheSecretTheTimestampOrTheBody() {
        String original = WebhookSigner.signature("whsec_test_secret", 1700000000L, BODY);

        assertNotEquals(original, WebhookSigner.signature("another_secret", 1700000000L, BODY));
        assertNotEquals(original, WebhookSigner.signature("whsec_test_secret", 1700000001L, BODY));
        assertNotEquals(original, WebhookSigner.signature("whsec_test_secret", 1700000000L,
                "{\"eventId\":\"e-2\"}".getBytes(StandardCharsets.UTF_8)));
    }
}
