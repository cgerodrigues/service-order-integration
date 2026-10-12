package io.github.cgerodrigues.serviceorderintegration.integration;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "management.otlp.metrics.export.enabled=false",
        "management.tracing.export.otlp.enabled=false",
        "management.logging.export.otlp.enabled=false",
        "logging.level.org.apache.kafka=WARN",
        "logging.level.kafka=WARN"
})
@EmbeddedKafka(partitions = 1, topics = {"service-order.created", "service-order.synchronized"},
        brokerProperties = {"log.dir=${java.io.tmpdir}/embedded-kafka-${random.uuid}"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ServiceOrderEndToEndIntegrationTests {
    private static final String TRACE_ID = "0123456789abcdef0123456789abcdef";
    private static final AtomicReference<String> httpTraceparent = new AtomicReference<>();
    private static final AtomicReference<String> httpBody = new AtomicReference<>();
    private static final AtomicInteger responseStatus = new AtomicInteger(201);
    private static final AtomicInteger httpRequests = new AtomicInteger();
    private static HttpServer server;

    @Autowired EmbeddedKafkaBroker broker;
    @Autowired JsonMapper mapper;
    @Autowired ConfigurableApplicationContext context;
    @Autowired KafkaListenerEndpointRegistry registry;
    @Autowired CommonErrorHandler errorHandler;
    @Autowired ProducerFactory<String, String> producerFactory;

    @BeforeEach
    void resetHttpResponse() {
        responseStatus.set(201);
        httpRequests.set(0);
        httpTraceparent.set(null);
        httpBody.set(null);
    }

    @DynamicPropertySource
    static void httpEndpoint(DynamicPropertyRegistry properties) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/service-orders", exchange -> {
            httpRequests.incrementAndGet();
            httpTraceparent.set(exchange.getRequestHeaders().getFirst("traceparent"));
            httpBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = "{\"externalOrderId\":\"external-1\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(responseStatus.get(), response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        properties.add("integration.field-service.base-url", () -> "http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void stopHttpServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void synchronizesThroughRealTransportsAndPropagatesW3cContext() throws Exception {
        var producerConfig = new ProducerConfig(producerFactory.getConfigurationProperties());
        assertThat(producerConfig.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isTrue();
        assertThat(producerConfig.getInt(ProducerConfig.RETRIES_CONFIG)).isGreaterThan(0);
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (var consumer = new KafkaConsumer<String, String>(config);
             var fixture = getClass().getResourceAsStream("/events/service-order-created.json")) {
            consumer.assign(List.of(new TopicPartition("service-order.synchronized", 0)));
            consumer.seekToBeginning(List.of(new TopicPartition("service-order.synchronized", 0)));
            assertThat(fixture).isNotNull();
            var created = new ProducerRecord<String, String>("service-order.created", "order-1",
                    new String(fixture.readAllBytes(), StandardCharsets.UTF_8));
            created.headers().add("traceparent", ("00-" + TRACE_ID + "-0123456789abcdef-01").getBytes(StandardCharsets.UTF_8));
            created.headers().add("content-type", "application/cloudevents+json".getBytes(StandardCharsets.UTF_8));
            try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(Map.of(
                    "bootstrap.servers", broker.getBrokersAsString(),
                    "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                    "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class))) {
                producer.send(created).get(10, TimeUnit.SECONDS);
            }
            org.apache.kafka.clients.consumer.ConsumerRecord<String, String> result = null;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (result == null && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) result = records.iterator().next();
            }
            assertThat(result).isNotNull();
            assertThat(result.key()).isEqualTo("order-1");
            var event = mapper.readTree(result.value());
            assertThat(event.get("correlationid").asString()).isEqualTo("flow-1");
            assertThat(event.get("causationid").asString()).isEqualTo("event-1");
            assertThat(event.get("data").get("externalOrderId").asString()).isEqualTo("external-1");
            assertThat(mapper.readTree(httpBody.get()).get("serviceOrderId").asString()).isEqualTo("order-1");
            assertThat(httpTraceparent.get()).startsWith("00-" + TRACE_ID + "-");
            var traceHeader = result.headers().lastHeader("traceparent");
            assertThat(traceHeader).isNotNull();
            assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).startsWith("00-" + TRACE_ID + "-");
        }
    }

    @Test
    void httpFailureStopsContainerWithoutSuccessOrCommittingFailedRecord() throws Exception {
        responseStatus.set(500);
        assertThat(errorHandler).isInstanceOf(CommonContainerStoppingErrorHandler.class);
        assertThat(registry.getListenerContainers()).hasSize(1);
        var container = registry.getListenerContainers().iterator().next();
        assertThat(container.getContainerProperties().getAckMode()).isEqualTo(ContainerProperties.AckMode.RECORD);
        ContainerTestUtils.waitForAssignment(container, 1);
        var stopped = new CountDownLatch(1);
        org.springframework.context.ApplicationListener<ConsumerStoppedEvent> observer = event -> stopped.countDown();
        context.addApplicationListener(observer);
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(Map.of(
                "bootstrap.servers", broker.getBrokersAsString(),
                "key.serializer", org.apache.kafka.common.serialization.StringSerializer.class,
                "value.serializer", org.apache.kafka.common.serialization.StringSerializer.class));
             var admin = Admin.create(Map.of("bootstrap.servers", broker.getBrokersAsString()))) {
            var created = new ProducerRecord<String, String>("service-order.created", "order-1",
                    io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.created());
            created.headers().add("content-type", "application/cloudevents+json".getBytes(StandardCharsets.UTF_8));
            var sent = producer.send(created).get(10, TimeUnit.SECONDS);
            assertThat(stopped.await(30, TimeUnit.SECONDS)).as("consumer closes after HTTP failure").isTrue();
            assertThat(container.isRunning()).isFalse();
            assertThat(container.isInExpectedState()).as("abnormal container stop").isFalse();
            assertThat(httpRequests.get()).as("no business retry").isEqualTo(1);

            var offsets = admin.listConsumerGroupOffsets(container.getGroupId())
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            var committed = offsets.get(new TopicPartition(sent.topic(), sent.partition()));
            // Kafka commits the NEXT offset. Null or <= the failed offset means it was not acknowledged.
            assertThat(committed == null || committed.offset() <= sent.offset())
                    .as("failed record must not be committed: %s", committed).isTrue();

            var successPartition = new TopicPartition("service-order.synchronized", 0);
            var end = admin.listOffsets(Map.of(successPartition,
                    org.apache.kafka.clients.admin.OffsetSpec.latest()))
                    .all().get(10, TimeUnit.SECONDS);
            assertThat(end.get(successPartition).offset()).as("no success event after HTTP failure").isZero();
        } finally {
            context.removeApplicationListener(observer);
        }
    }
}
