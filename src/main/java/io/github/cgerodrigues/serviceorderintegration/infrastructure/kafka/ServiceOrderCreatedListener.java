package io.github.cgerodrigues.serviceorderintegration.infrastructure.kafka;

import java.nio.charset.StandardCharsets;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizeServiceOrder;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;

@Component
public final class ServiceOrderCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(ServiceOrderCreatedListener.class);
    private final ServiceOrderCloudEvents events;
    private final Validator validator;
    private final SynchronizeServiceOrder useCase;

    public ServiceOrderCreatedListener(ServiceOrderCloudEvents events, Validator validator,
                                      SynchronizeServiceOrder useCase) {
        this.events = events;
        this.validator = validator;
        this.useCase = useCase;
    }

    @KafkaListener(topics = "${integration.kafka.inbound-topic}")
    public void consume(ConsumerRecord<String, String> record) {
        validateContentType(record);
        var event = events.readCreated(record.value());
        String previous = MDC.get("correlationId");
        MDC.put("correlationId", event.metadata().correlationId());
        try {
            var violations = validator.validate(event.order());
            if (!violations.isEmpty()) {
                throw new ConstraintViolationException(violations);
            }
            if (record.key() != null && !record.key().equals(event.order().id())) {
                throw new IllegalArgumentException("Kafka key must equal the service order id");
            }
            useCase.synchronize(event.order(), event.metadata());
            log.info("Service order synchronized: {}", event.order().id());
        } catch (RuntimeException ex) {
            log.error("Service order synchronization failed: {}", event.order().id(), ex);
            throw ex;
        } finally {
            if (previous == null) {
                MDC.remove("correlationId");
            } else {
                MDC.put("correlationId", previous);
            }
        }
    }

    private static void validateContentType(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader("content-type");
        if (header == null || header.value() == null) {
            throw new IllegalArgumentException("Kafka content-type header is required for structured CloudEvents");
        }
        MediaType contentType;
        try {
            contentType = MediaType.parseMediaType(new String(header.value(), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Invalid Kafka content-type header for structured CloudEvents", ex);
        }
        if (!"application".equalsIgnoreCase(contentType.getType())
                || !"cloudevents+json".equalsIgnoreCase(contentType.getSubtype())) {
            throw new IllegalArgumentException("Kafka content-type must be application/cloudevents+json");
        }
    }
}
