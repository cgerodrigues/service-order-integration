package io.github.cgerodrigues.serviceorderintegration.infrastructure.kafka;

import java.nio.charset.StandardCharsets;

import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.FieldServicePort;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizationEventPort;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizeServiceOrder;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;

import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ServiceOrderCreatedListenerTests {
    private static ValidatorFactory validators;

    @BeforeAll
    static void createValidator() { validators = Validation.buildDefaultValidatorFactory(); }

    @AfterAll
    static void closeValidator() { validators.close(); }

    @ParameterizedTest
    @ValueSource(strings = {"application/cloudevents+json", "application/cloudevents+json; charset=UTF-8",
            "Application/CloudEvents+Json ; charset=\"UTF-8\""})
    void acceptsStructuredMediaTypeAndSupportedEvent(String contentType) {
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        listener.consume(record(contentType, created()));
        var event = EVENTS.readCreated(created());
        verify(useCase).synchronize(event.order(), event.metadata());
    }

    @Test
    void rejectsMissingHeaderBeforeDeserializing() {
        var events = mock(ServiceOrderCloudEvents.class);
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(events, validators.getValidator(), useCase);
        var record = new ConsumerRecord<String, String>("created", 0, 0, "order-1", "not-json");
        assertThatThrownBy(() -> listener.consume(record)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("content-type header is required");
        verifyNoInteractions(events, useCase);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "not-a-media-type", "application/json", "text/plain",
            "application/cloudevents+avro", "*/*", "application/*", "application/*+json"})
    void rejectsInvalidOrUnsupportedHeaderBeforeDeserializing(String contentType) {
        var events = mock(ServiceOrderCloudEvents.class);
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(events, validators.getValidator(), useCase);
        assertThatThrownBy(() -> listener.consume(record(contentType, "not-json")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("content-type");
        verifyNoInteractions(events, useCase);
    }

    @Test
    void rejectsUnsupportedEventEvenWithCorrectTransportHeader() {
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        assertThatThrownBy(() -> listener.consume(record(ServiceOrderCloudEvents.CONTENT_TYPE,
                created().replace(ServiceOrderCloudEvents.CREATED, "unsupported.v1"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported CloudEvent type");
        verifyNoInteractions(useCase);
    }

    @Test
    void preservesDataContentTypeValidationIndependentlyOfTransportHeader() {
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        assertThatThrownBy(() -> listener.consume(record(ServiceOrderCloudEvents.CONTENT_TYPE,
                created().replace("application/json", "application/cloudevents+json"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("data must be application/json");
        verifyNoInteractions(useCase);
    }

    @Test
    void deliveryFailurePropagatesWithoutSuccessAndRestoresMdc() {
        var delivery = mock(FieldServicePort.class);
        var publisher = mock(SynchronizationEventPort.class);
        var failure = new IllegalStateException("HTTP 500");
        when(delivery.createOrder(any())).thenAnswer(invocation -> {
            assertThat(MDC.get("correlationId")).isEqualTo("flow-1");
            throw failure;
        });
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(),
                new SynchronizeServiceOrder(delivery, publisher));
        MDC.put("correlationId", "outer-flow");
        try {
            assertThatThrownBy(() -> listener.consume(record(ServiceOrderCloudEvents.CONTENT_TYPE, created())))
                    .isSameAs(failure);
            verifyNoInteractions(publisher);
            assertThat(MDC.get("correlationId")).isEqualTo("outer-flow");
        } finally {
            MDC.remove("correlationId");
        }
    }

    private static ConsumerRecord<String, String> record(String contentType, String json) {
        var record = new ConsumerRecord<String, String>("created", 0, 0, "order-1", json);
        record.headers().add("content-type", contentType == null ? null : contentType.getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
