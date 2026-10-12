package io.github.cgerodrigues.serviceorderintegration.infrastructure.kafka;

import java.util.concurrent.CompletableFuture;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import org.springframework.http.MediaType;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.*;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.kafka.*;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;
import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;

class ServiceOrderFlowTests {
    private static ValidatorFactory validators;

    @BeforeAll
    static void setupValidation() { validators = Validation.buildDefaultValidatorFactory(); }
    @AfterAll
    static void closeValidation() { validators.close(); }
    @AfterEach
    void clearMdc() { MDC.clear(); }

    @Test
    @SuppressWarnings("unchecked")
    void synchronizesFromKafkaRecordThroughHttpToConfirmedKafkaPublication() {
        RestClient.Builder builder = RestClient.builder();
        var server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://field-service/service-orders"))
                .andExpect(method(org.springframework.http.HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.serviceOrderId").value("order-1"))
                .andExpect(jsonPath("$.customer.id").value("customer-1"))
                .andExpect(request -> assertThat(MDC.get("correlationId")).isEqualTo("flow-1"))
                .andRespond(withSuccess("{\"externalOrderId\":\"external-1\"}", MediaType.APPLICATION_JSON));
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
        var publisher = new KafkaSynchronizationEventPublisher(kafka, EVENTS, "service-order.synchronized");
        var useCase = new SynchronizeServiceOrder(new HttpFieldServiceAdapter(builder.baseUrl("http://field-service").build()), publisher);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        MDC.put("correlationId", "outer-context");
        listener.consume(record("order-1", created()));
        ArgumentCaptor<ProducerRecord<String, String>> captured = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafka).send(captured.capture());
        var record = captured.getValue();
        assertThat(record.key()).isEqualTo("order-1");
        assertThat(record.topic()).isEqualTo("service-order.synchronized");
        assertThat(new String(record.headers().lastHeader("content-type").value(), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("application/cloudevents+json");
        var event = MAPPER.readTree(record.value());
        assertThat(event.get("correlationid").asString()).isEqualTo("flow-1");
        assertThat(event.get("causationid").asString()).isEqualTo("event-1");
        assertThat(event.get("data").get("externalOrderId").asString()).isEqualTo("external-1");
        assertThat(MDC.get("correlationId")).isEqualTo("outer-context");
        server.verify();
    }

    @Test
    void validationFailureDoesNotInvokeUseCaseAndCleansLogContext() {
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        String invalid = created().replace("Alex Smith", " ");
        assertThatThrownBy(() -> listener.consume(record("order-1", invalid)))
                .isInstanceOf(ConstraintViolationException.class).hasMessageContaining("customer.name");
        verifyNoInteractions(useCase);
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void mismatchedPartitionKeyIsRejected() {
        var useCase = mock(SynchronizeServiceOrder.class);
        var listener = new ServiceOrderCreatedListener(EVENTS, validators.getValidator(), useCase);
        assertThatThrownBy(() -> listener.consume(record("another-order", created())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Kafka key");
        verifyNoInteractions(useCase);
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedBrokerConfirmationFailsPublication() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")));
        var publisher = new KafkaSynchronizationEventPublisher(kafka, EVENTS, "success");
        assertThatThrownBy(() -> publisher.publish("order-1", "external-1", new EventMetadata("event-1", "flow-1")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("publication failed");
    }

    private static ConsumerRecord<String, String> record(String key, String json) {
        var record = new ConsumerRecord<String, String>("service-order.created", 0, 0, key, json);
        record.headers().add("content-type", "application/cloudevents+json".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return record;
    }
}
