package io.github.cgerodrigues.serviceorderintegration.application.serviceorder;

import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;

/** Coordinates delivery and success notification for an already validated order. */
public final class SynchronizeServiceOrder {
    private final FieldServicePort fieldService;
    private final SynchronizationEventPort events;

    public SynchronizeServiceOrder(FieldServicePort fieldService, SynchronizationEventPort events) {
        this.fieldService = fieldService;
        this.events = events;
    }

    public void synchronize(ServiceOrder order, EventMetadata metadata) {
        String externalOrderId = fieldService.createOrder(order);
        if (externalOrderId == null || externalOrderId.isBlank()) {
            throw new IllegalStateException("Field Service returned no external order identifier");
        }
        events.publish(order.id(), externalOrderId, metadata);
    }
}
