package io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;
import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;

class HttpFieldServiceAdapterTests {
    @Test
    void mapsEveryFieldToDedicatedExternalContract() {
        var order = EVENTS.readCreated(created()).order();
        var request = FieldServiceOrderRequest.from(order);
        var expected = MAPPER.readTree("""
                {
                  "serviceOrderId": "order-1",
                  "name": "Install connection",
                  "type": "INSTALLATION",
                  "description": "Install the requested connection",
                  "customer": {"id": "customer-1", "name": "Alex Smith"},
                  "serviceAddress": {
                    "street": "Main Street", "number": "10", "complement": "Apartment 2",
                    "district": "Centro", "city": "Fortaleza", "state": "CE",
                    "postalCode": "60000-000", "country": "BR"
                  },
                  "priority": "NORMAL",
                  "requestedWindow": {
                    "start": "2026-10-12T09:00:00-03:00", "end": "2026-10-12T11:00:00-03:00"
                  },
                  "notes": "Call on arrival"
                }
                """);
        assertThat((tools.jackson.databind.JsonNode) MAPPER.valueToTree(request)).isEqualTo(expected);
        assertThat(request.customer().getClass()).isNotEqualTo(order.customer().getClass());
        assertThat(request.serviceAddress().getClass()).isNotEqualTo(order.serviceAddress().getClass());
        assertThat(request.requestedWindow().getClass()).isNotEqualTo(order.requestedWindow().getClass());
    }

    @Test
    void mapsAbsentOptionalWindow() {
        var tree = (tools.jackson.databind.node.ObjectNode) MAPPER.readTree(created());
        ((tools.jackson.databind.node.ObjectNode) tree.get("data")).remove("requestedWindow");
        assertThat(FieldServiceOrderRequest.from(EVENTS.readCreated(tree.toString()).order()).requestedWindow()).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"externalOrderId\":\" \"}", "null"})
    void rejectsInvalidSuccessResponse(String body) {
        var builder = RestClient.builder();
        var server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://field-service/service-orders")).andRespond(withSuccess(body, MediaType.APPLICATION_JSON));
        var adapter = new HttpFieldServiceAdapter(builder.baseUrl("http://field-service").build());
        assertThatThrownBy(() -> adapter.createOrder(EVENTS.readCreated(created()).order())).isInstanceOf(IllegalStateException.class);
        server.verify();
    }

    @Test
    void propagatesHttpFailure() {
        var builder = RestClient.builder();
        var server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://field-service/service-orders")).andRespond(withServerError());
        var adapter = new HttpFieldServiceAdapter(builder.baseUrl("http://field-service").build());
        assertThatThrownBy(() -> adapter.createOrder(EVENTS.readCreated(created()).order()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("HTTP 500");
        server.verify();
    }
}
