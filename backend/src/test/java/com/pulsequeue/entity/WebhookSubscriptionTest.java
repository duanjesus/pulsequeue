package com.pulsequeue.entity;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WebhookSubscriptionTest {

    @Test
    void wildcardMatchesEveryEventType() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "*");

        assertTrue(subscription.matches("expense.created"));
        assertTrue(subscription.matches("donation.created"));
    }

    @Test
    void trailingWildcardMatchesByPrefix() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "expense.*");

        assertTrue(subscription.matches("expense.created"));
        assertFalse(subscription.matches("donation.created"));
    }

    @Test
    void patternWithoutWildcardMustBeEqual() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "expense.created");

        assertTrue(subscription.matches("expense.created"));
        assertFalse(subscription.matches("expense.created.v2"));
    }

    @Test
    void isDisabledOnlyAfterTheConfiguredNumberOfConsecutiveFailures() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "*");

        subscription.registerFailure(3);
        subscription.registerFailure(3);
        assertTrue(subscription.isActive());

        subscription.registerFailure(3);
        assertFalse(subscription.isActive());
        assertEquals(3, subscription.getConsecutiveFailures());
    }

    @Test
    void aSuccessResetsTheFailureStreak() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "*");
        subscription.registerFailure(3);
        subscription.registerFailure(3);

        subscription.registerSuccess();
        subscription.registerFailure(3);

        assertTrue(subscription.isActive());
        assertEquals(1, subscription.getConsecutiveFailures());
    }

    @Test
    void enablingClearsTheStreakAndReactivates() {
        WebhookSubscription subscription = WebhookSubscription.of("http://receiver/hook", "s", "*");
        subscription.registerFailure(1);
        assertFalse(subscription.isActive());

        subscription.enable();

        assertTrue(subscription.isActive());
        assertEquals(0, subscription.getConsecutiveFailures());
    }
}
