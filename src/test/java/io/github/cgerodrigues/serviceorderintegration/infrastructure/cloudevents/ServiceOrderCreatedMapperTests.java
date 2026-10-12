package io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents;

import java.time.OffsetDateTime;

import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.Customer;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.RequestedWindow;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceAddress;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrderPriority;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrderType;

import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;
import static org.assertj.core.api.Assertions.*;

class ServiceOrderCreatedMapperTests {
    @Test
    void mapsCompleteV1PayloadToIndependentDomainRecords() {
        var payload = new ServiceOrderCreatedData("order-1", "Install connection",
                ServiceOrderCreatedData.Type.INSTALLATION, "Description",
                new ServiceOrderCreatedData.CustomerData("customer-1", "Alex"),
                new ServiceOrderCreatedData.ServiceAddressData("Main", "10", "Apt 2", "Centro",
                        "Fortaleza", "CE", "60000-000", "BR"), ServiceOrderCreatedData.Priority.NORMAL,
                new ServiceOrderCreatedData.RequestedWindowData(
                        OffsetDateTime.parse("2026-10-12T09:00:00-03:00"),
                        OffsetDateTime.parse("2026-10-12T11:00:00-03:00")), "Call on arrival");
        var expected = new ServiceOrder("order-1", "Install connection", ServiceOrderType.INSTALLATION,
                "Description", new Customer("customer-1", "Alex"),
                new ServiceAddress("Main", "10", "Apt 2", "Centro", "Fortaleza", "CE", "60000-000", "BR"),
                ServiceOrderPriority.NORMAL,
                new RequestedWindow(OffsetDateTime.parse("2026-10-12T09:00:00-03:00"),
                        OffsetDateTime.parse("2026-10-12T11:00:00-03:00")), "Call on arrival");
        assertThat(ServiceOrderCreatedMapper.toDomain(payload)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"INSTALLATION,LOW", "MAINTENANCE,NORMAL", "REPAIR,HIGH", "INSPECTION,URGENT"})
    void explicitlyMapsAllV1EnumValues(ServiceOrderCreatedData.Type type, ServiceOrderCreatedData.Priority priority) {
        var payload = new ServiceOrderCreatedData("id", "name", type, null, null, null, priority, null, null);
        var order = ServiceOrderCreatedMapper.toDomain(payload);
        assertThat(order.type()).isEqualTo(ServiceOrderType.valueOf(type.name()));
        assertThat(order.priority()).isEqualTo(ServiceOrderPriority.valueOf(priority.name()));
    }

    @Test
    void preservesMissingRequiredValuesForDomainValidationAndAllowsAbsentOptionalValues() {
        var payload = new ServiceOrderCreatedData("id", "name", null, null, null, null, null, null, null);
        var order = ServiceOrderCreatedMapper.toDomain(payload);
        assertThat(order.requestedWindow()).isNull();
        assertThat(order.description()).isNull();
        assertThat(order.notes()).isNull();
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(order)).extracting(v -> v.getPropertyPath().toString())
                    .containsExactlyInAnyOrder("type", "customer", "serviceAddress", "priority");
        }
    }

    @Test
    void preservesRequestedWindowDomainInvariant() {
        var payload = new ServiceOrderCreatedData("id", "name", null, null, null, null, null,
                new ServiceOrderCreatedData.RequestedWindowData(OffsetDateTime.parse("2026-10-12T11:00:00Z"),
                        OffsetDateTime.parse("2026-10-12T09:00:00Z")), null);
        assertThatThrownBy(() -> ServiceOrderCreatedMapper.toDomain(payload))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("strictly after start");
    }

    @Test
    void deserializesV1NestedObjectsIntoTransportTypes() {
        var data = MAPPER.treeToValue(MAPPER.readTree(created()).get("data"), ServiceOrderCreatedData.class);
        assertThat(data.customer()).isInstanceOf(ServiceOrderCreatedData.CustomerData.class);
        assertThat(data.serviceAddress()).isInstanceOf(ServiceOrderCreatedData.ServiceAddressData.class);
        assertThat(data.requestedWindow()).isInstanceOf(ServiceOrderCreatedData.RequestedWindowData.class);
    }
}
