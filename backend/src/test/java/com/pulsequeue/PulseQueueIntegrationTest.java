package com.pulsequeue;

import com.fasterxml.jackson.databind.JsonNode;
import com.pulsequeue.entity.DeliveryStatus;
import com.pulsequeue.entity.NotificationDelivery;
import com.pulsequeue.entity.ProcessedEvent;
import com.pulsequeue.entity.ProcessedEventStatus;
import com.pulsequeue.event.DomainEvent;
import com.pulsequeue.notification.NotificationChannel;
import com.pulsequeue.producer.EventPublisher;
import com.pulsequeue.repository.NotificationDeliveryRepository;
import com.pulsequeue.repository.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end proof that the pipeline actually works against real
 * Postgres/RabbitMQ/Redis, not just mocked collaborators: publish -> queue
 * -> consumer -> dedup/notify -> recorded outcome, including the retry+DLQ
 * path and the duplicate-delivery path.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PulseQueueIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("pulsequeue_db")
            .withUsername("pulsequeue_user")
            .withPassword("pulsequeue_pass");

    @Container
    static final RabbitMQContainer RABBITMQ = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management-alpine"));

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Container
    static final GenericContainer<?> MAILPIT = new GenericContainer<>(DockerImageName.parse("axllent/mailpit:v1.31.3"))
            .withExposedPorts(1025, 8025)
            .waitingFor(Wait.forHttp("/readyz").forPort(8025));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("spring.rabbitmq.host", RABBITMQ::getHost);
        registry.add("spring.rabbitmq.port", RABBITMQ::getAmqpPort);
        registry.add("spring.rabbitmq.username", RABBITMQ::getAdminUsername);
        registry.add("spring.rabbitmq.password", RABBITMQ::getAdminPassword);

        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));

        registry.add("spring.mail.host", MAILPIT::getHost);
        registry.add("spring.mail.port", () -> MAILPIT.getMappedPort(1025));

        // Shrink retry backoff for the test run: the default (500ms/2x/5s max,
        // ~1.5s worst case) is tuned for real usage, not for minimizing how
        // long a test needs 3 Testcontainers instances to stay healthy under
        // a resource-constrained CI runner. Finishing the retry-to-DLQ cycle
        // in tens of milliseconds instead of over a second meaningfully
        // shrinks the window for a transient container hiccup to land
        // mid-test.
        registry.add("pulsequeue.retry.initial-interval-ms", () -> "20");
        registry.add("pulsequeue.retry.multiplier", () -> "1.5");
        registry.add("pulsequeue.retry.max-interval-ms", () -> "100");
    }

    /** Counts sends per event, and fails the first one for events flagged {@code failFirstSend}. */
    static class FlakyOnceChannel implements NotificationChannel {

        private final Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();

        @Override
        public String getChannelName() {
            return "TEST_FLAKY";
        }

        @Override
        public List<String> targetsFor(DomainEvent event) {
            return Boolean.TRUE.equals(event.payload().get("failFirstSend")) ? List.of("flaky") : List.of();
        }

        @Override
        public void send(DomainEvent event, String target) {
            if (sends.computeIfAbsent(event.eventId(), id -> new AtomicInteger()).incrementAndGet() == 1) {
                throw new IllegalStateException("first send always fails");
            }
        }
    }

    /** Counts sends per event and never fails. */
    static class CountingChannel implements NotificationChannel {

        private final Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();

        @Override
        public String getChannelName() {
            return "TEST_COUNTER";
        }

        @Override
        public List<String> targetsFor(DomainEvent event) {
            return List.of("counter");
        }

        @Override
        public void send(DomainEvent event, String target) {
            sends.computeIfAbsent(event.eventId(), id -> new AtomicInteger()).incrementAndGet();
        }

        int sendsFor(String eventId) {
            AtomicInteger count = sends.get(eventId);
            return count == null ? 0 : count.get();
        }
    }

    @TestConfiguration
    static class TestChannels {

        @Bean
        FlakyOnceChannel flakyOnceChannel() {
            return new FlakyOnceChannel();
        }

        @Bean
        CountingChannel countingChannel() {
            return new CountingChannel();
        }
    }

    @Autowired
    private EventPublisher publisher;
    @Autowired
    private ProcessedEventRepository repository;
    @Autowired
    private NotificationDeliveryRepository deliveryRepository;
    @Autowired
    private CountingChannel countingChannel;

    @Test
    void publishedEventIsConsumedAndMarkedProcessed() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 42));

        publisher.publish(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<ProcessedEvent> record = repository.findByEventId(event.eventId());
            assertTrue(record.isPresent(), "event should have been recorded by now");
            assertEquals(ProcessedEventStatus.PROCESSED, record.get().getStatus());
        });
    }

    @Test
    void eventThatAlwaysFailsExhaustsRetriesAndLandsOnTheDeadLetterQueue() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("simulateFailure", true));

        publisher.publish(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<ProcessedEvent> record = repository.findByEventId(event.eventId());
            assertTrue(record.isPresent(), "event should have been recorded by now");
            assertEquals(ProcessedEventStatus.DEAD_LETTERED, record.get().getStatus());
            assertTrue(record.get().getRetryCount() >= 1);
        });
    }

    @Test
    void retryResendsOnlyTheDestinationThatFailed() {
        String recipient = UUID.randomUUID() + "@cashpilot.example";
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot",
                Map.of("failFirstSend", true, "recipientEmail", recipient));

        publisher.publish(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<ProcessedEvent> record = repository.findByEventId(event.eventId());
            assertTrue(record.isPresent(), "event should have been recorded by now");
            assertEquals(ProcessedEventStatus.PROCESSED, record.get().getStatus());
            assertEquals(1, record.get().getRetryCount());
        });

        assertEquals(1, countingChannel.sendsFor(event.eventId()),
                "a destination delivered on the first attempt must not be sent again on the retry");

        Map<String, NotificationDelivery> deliveries = deliveryRepository.findByEventIdOrderByIdAsc(event.eventId())
                .stream().collect(Collectors.toMap(NotificationDelivery::getChannel, Function.identity()));
        assertTrue(deliveries.keySet().containsAll(List.of("EMAIL", "PUSH", "WEBSOCKET", "TEST_COUNTER", "TEST_FLAKY")),
                "every channel that applied should have a delivery row, got " + deliveries.keySet());
        deliveries.values().forEach(delivery ->
                assertEquals(DeliveryStatus.DELIVERED, delivery.getStatus(), delivery.getChannel()));
        assertEquals(2, deliveries.get("TEST_FLAKY").getAttempts());
        assertEquals(1, deliveries.get("TEST_COUNTER").getAttempts());
        assertEquals(1, deliveries.get("EMAIL").getAttempts());
        assertEquals(recipient, deliveries.get("EMAIL").getTarget());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertEquals(1, emailsReceivedBy(recipient), "the real inbox must hold exactly one copy"));
    }

    @Test
    void eventWithoutRecipientSendsNoEmail() {
        DomainEvent event = DomainEvent.of("expense.created", "cashpilot", Map.of("amount", 7));

        publisher.publish(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(ProcessedEventStatus.PROCESSED,
                        repository.findByEventId(event.eventId()).map(ProcessedEvent::getStatus).orElse(null)));
        assertTrue(deliveryRepository.findByEventIdOrderByIdAsc(event.eventId()).stream()
                .noneMatch(delivery -> delivery.getChannel().equals("EMAIL")));
    }

    private long emailsReceivedBy(String recipient) {
        JsonNode inbox = RestClient.create()
                .get()
                .uri("http://{host}:{port}/api/v1/messages?limit=200", MAILPIT.getHost(), MAILPIT.getMappedPort(8025))
                .retrieve()
                .body(JsonNode.class);
        long count = 0;
        for (JsonNode message : inbox.path("messages")) {
            for (JsonNode to : message.path("To")) {
                if (recipient.equals(to.path("Address").asText())) {
                    count++;
                }
            }
        }
        return count;
    }

    @Test
    void duplicateEventIdIsSkippedOnSecondDelivery() {
        String eventId = UUID.randomUUID().toString();
        DomainEvent event = new DomainEvent(eventId, "expense.created", "cashpilot", Map.of("amount", 1), Instant.now());

        publisher.publish(event);
        await().atMost(Duration.ofSeconds(10)).until(() ->
                repository.findByEventId(eventId).map(r -> r.getStatus() == ProcessedEventStatus.PROCESSED).orElse(false));

        publisher.publish(event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<ProcessedEvent> record = repository.findByEventId(eventId);
            assertTrue(record.isPresent(), "duplicate delivery should still have a record");
            assertEquals(ProcessedEventStatus.DUPLICATE, record.get().getStatus());
        });
    }
}
