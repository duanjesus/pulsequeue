package com.pulsequeue.service;

import com.pulsequeue.config.WebhookProperties;
import com.pulsequeue.entity.WebhookSubscription;
import com.pulsequeue.repository.WebhookSubscriptionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

@Service
public class WebhookSubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(WebhookSubscriptionService.class);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final WebhookSubscriptionRepository repository;
    private final int maxConsecutiveFailures;

    public WebhookSubscriptionService(WebhookSubscriptionRepository repository, WebhookProperties properties) {
        this.repository = repository;
        this.maxConsecutiveFailures = properties.maxConsecutiveFailures();
    }

    @Transactional
    public WebhookSubscription create(String url, String eventTypePattern) {
        String pattern = eventTypePattern == null || eventTypePattern.isBlank()
                ? WebhookSubscription.MATCH_ALL : eventTypePattern.trim();
        return repository.save(WebhookSubscription.of(validated(url), newSecret(), pattern));
    }

    @Transactional(readOnly = true)
    public List<WebhookSubscription> findAll() {
        return repository.findAllByOrderByIdAsc();
    }

    @Transactional(readOnly = true)
    public List<WebhookSubscription> findActiveMatching(String eventType) {
        return repository.findByActiveTrue().stream()
                .filter(subscription -> subscription.matches(eventType))
                .toList();
    }

    @Transactional(readOnly = true)
    public Optional<WebhookSubscription> findActive(Long id) {
        return repository.findById(id).filter(WebhookSubscription::isActive);
    }

    @Transactional
    public boolean delete(Long id) {
        if (!repository.existsById(id)) {
            return false;
        }
        repository.deleteById(id);
        return true;
    }

    @Transactional
    public Optional<WebhookSubscription> enable(Long id) {
        return repository.findById(id).map(subscription -> {
            subscription.enable();
            return subscription;
        });
    }

    @Transactional
    public void recordSuccess(Long id) {
        repository.findById(id)
                .filter(subscription -> subscription.getConsecutiveFailures() > 0)
                .ifPresent(WebhookSubscription::registerSuccess);
    }

    @Transactional
    public void recordFailure(Long id) {
        repository.findById(id).ifPresent(subscription -> {
            subscription.registerFailure(maxConsecutiveFailures);
            if (!subscription.isActive()) {
                log.warn("Webhook subscription {} ({}) disabled after {} consecutive failures",
                        id, subscription.getUrl(), subscription.getConsecutiveFailures());
            }
        });
    }

    /**
     * Only checks that the URL is a well-formed http(s) address. It does not
     * block private or loopback hosts, so anyone holding the API key can make
     * this service call internal addresses — acceptable for a demo where the
     * receiver itself lives on the Docker network, not for a public deployment.
     */
    private String validated(String url) {
        try {
            URI uri = new URI(url.trim());
            boolean http = "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
            if (!http || uri.getHost() == null) {
                throw new IllegalArgumentException("Webhook URL must be an absolute http or https URL");
            }
            return uri.toString();
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("Webhook URL is not a valid URL");
        }
    }

    private String newSecret() {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        return "whsec_" + HexFormat.of().formatHex(bytes);
    }
}
