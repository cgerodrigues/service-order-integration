package io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice;

import java.time.OffsetDateTime;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;

public record FieldServiceOrderRequest(String serviceOrderId, String name, String type,
        String description, CustomerRequest customer, AddressRequest serviceAddress,
        String priority, WindowRequest requestedWindow, String notes) {

    public static FieldServiceOrderRequest from(ServiceOrder order) {
        var customer = order.customer();
        var address = order.serviceAddress();
        var window = order.requestedWindow();
        return new FieldServiceOrderRequest(order.id(), order.name(), order.type().name(),
                order.description(), new CustomerRequest(customer.id(), customer.name()),
                new AddressRequest(address.street(), address.number(), address.complement(),
                        address.district(), address.city(), address.state(), address.postalCode(), address.country()),
                order.priority().name(), window == null ? null : new WindowRequest(window.start(), window.end()),
                order.notes());
    }

    public record CustomerRequest(String id, String name) { }
    public record AddressRequest(String street, String number, String complement, String district,
                                 String city, String state, String postalCode, String country) { }
    public record WindowRequest(OffsetDateTime start, OffsetDateTime end) { }
}
