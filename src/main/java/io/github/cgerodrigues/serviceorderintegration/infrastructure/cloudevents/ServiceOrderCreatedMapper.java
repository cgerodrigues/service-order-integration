package io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents;

import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.Customer;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.RequestedWindow;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceAddress;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrderPriority;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrderType;

/** Maps the v1 transport schema without making the domain depend on that schema. */
public final class ServiceOrderCreatedMapper {
    private ServiceOrderCreatedMapper() { }

    public static ServiceOrder toDomain(ServiceOrderCreatedData data) {
        var customer = data.customer();
        var address = data.serviceAddress();
        var window = data.requestedWindow();
        // Preserve nulls so recursive domain Bean Validation reports missing required values.
        return new ServiceOrder(data.id(), data.name(), mapType(data.type()), data.description(),
                customer == null ? null : new Customer(customer.id(), customer.name()),
                address == null ? null : new ServiceAddress(address.street(), address.number(),
                        address.complement(), address.district(), address.city(), address.state(),
                        address.postalCode(), address.country()),
                mapPriority(data.priority()),
                window == null ? null : new RequestedWindow(window.start(), window.end()), data.notes());
    }

    private static ServiceOrderType mapType(ServiceOrderCreatedData.Type type) {
        return switch (type) {
            case null -> null;
            case INSTALLATION -> ServiceOrderType.INSTALLATION;
            case MAINTENANCE -> ServiceOrderType.MAINTENANCE;
            case REPAIR -> ServiceOrderType.REPAIR;
            case INSPECTION -> ServiceOrderType.INSPECTION;
        };
    }

    private static ServiceOrderPriority mapPriority(ServiceOrderCreatedData.Priority priority) {
        return switch (priority) {
            case null -> null;
            case LOW -> ServiceOrderPriority.LOW;
            case NORMAL -> ServiceOrderPriority.NORMAL;
            case HIGH -> ServiceOrderPriority.HIGH;
            case URGENT -> ServiceOrderPriority.URGENT;
        };
    }
}
