package com.pulsequeue.notification;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

/**
 * Signs a webhook delivery so the receiver can prove it came from PulseQueue
 * and was not altered: HMAC-SHA256 over {@code "<timestamp>.<raw body>"} with
 * the subscription's secret. Including the timestamp lets the receiver reject
 * replays of an old, otherwise valid request.
 */
public final class WebhookSigner {

    public static final String SIGNATURE_HEADER = "X-PulseQueue-Signature";

    private static final String ALGORITHM = "HmacSHA256";

    private WebhookSigner() {
    }

    public static String signature(String secret, long timestamp, byte[] body) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            mac.update((timestamp + ".").getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(mac.doFinal(body));
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("HMAC-SHA256 is unavailable", ex);
        }
    }

    /** Header value in the form {@code t=<unix seconds>,v1=<hex signature>}. */
    public static String header(String secret, long timestamp, byte[] body) {
        return "t=" + timestamp + ",v1=" + signature(secret, timestamp, body);
    }
}
