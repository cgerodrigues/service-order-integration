package io.github.cgerodrigues.serviceorderintegration.application.serviceorder;

import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;

public interface FieldServicePort {
    String createOrder(ServiceOrder order);
}
