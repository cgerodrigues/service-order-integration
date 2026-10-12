package io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.EventMetadata;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;
import static org.assertj.core.api.Assertions.*;
import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;

class ServiceOrderCloudEventsTests {
    @Test
    void readsStructuredEventAndKeepsEntityEventAndCorrelationIdsDistinct() {
        var event = EVENTS.readCreated(created());
        assertThat(event.order().id()).isEqualTo("order-1");
        assertThat(event.order().requestedWindow().start().getOffset().toString()).isEqualTo("-03:00");
        assertThat(event.metadata()).isEqualTo(new EventMetadata("event-1", "flow-1"));
    }

    @Test
    void rejectsUnsupportedType() {
        assertThatThrownBy(() -> EVENTS.readCreated(created().replace(ServiceOrderCloudEvents.CREATED, "other.v1")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported");
    }

    @ParameterizedTest
    @ValueSource(strings = {"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "correlationid", "data"})
    void requiresAllContractFields(String field) {
        var node = (tools.jackson.databind.node.ObjectNode) MAPPER.readTree(created());
        node.remove(field);
        assertThatThrownBy(() -> EVENTS.readCreated(node.toString())).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "[]", "{", "", "{}"})
    void rejectsMalformedEnvelopes(String json) {
        assertThatThrownBy(() -> EVENTS.readCreated(json)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "correlationid"})
    void rejectsNonStringAttributes(String field) {
        var node = (tools.jackson.databind.node.ObjectNode) MAPPER.readTree(created());
        node.put(field, 123);
        assertThatThrownBy(() -> EVENTS.readCreated(node.toString())).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"application/xml", "text/plain"})
    void rejectsNonJsonData(String contentType) {
        assertThatThrownBy(() -> EVENTS.readCreated(created().replace("application/json", contentType)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsTrailingJsonAndDuplicateAttributes() {
        assertThatThrownBy(() -> EVENTS.readCreated(created() + " {}"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EVENTS.readCreated(created().replaceFirst("\\{", "{\"id\":\"duplicate\",")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void constructsSuccessWithNewIdAndPreservesCorrelationAndCausation() {
        var metadata = new EventMetadata("event-1", "flow-1");
        var event = MAPPER.readTree(EVENTS.writeSynchronized("order-1", "external-1", metadata));
        assertThat(UUID.fromString(event.get("id").asString())).isNotNull();
        assertThat(event.get("id").asString()).isNotEqualTo("event-1");
        assertThat(event.get("id")).isNotEqualTo(MAPPER.readTree(EVENTS.writeSynchronized("order-1", "external-1", metadata)).get("id"));
        assertThat(event.get("specversion").asString()).isEqualTo("1.0");
        assertThat(event.get("type").asString()).isEqualTo(ServiceOrderCloudEvents.SYNCHRONIZED);
        assertThat(event.get("source").asString()).isEqualTo("urn:service-order-integration");
        assertThat(event.get("subject").asString()).isEqualTo("order-1");
        assertThat(event.get("time").asString()).isEqualTo("2026-10-12T12:00:00Z");
        assertThat(event.get("correlationid").asString()).isEqualTo("flow-1");
        assertThat(event.get("causationid").asString()).isEqualTo("event-1");
        assertThat(event.get("data")).isEqualTo(MAPPER.readTree("""
                {"serviceOrderId":"order-1","externalOrderId":"external-1","status":"SYNCHRONIZED"}
                """));
    }
}
