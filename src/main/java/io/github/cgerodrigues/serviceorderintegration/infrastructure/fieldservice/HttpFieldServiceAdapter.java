package io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice;

import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.FieldServicePort;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;

public final class HttpFieldServiceAdapter implements FieldServicePort {
    private final RestClient client;

    public HttpFieldServiceAdapter(RestClient client) {
        this.client = client;
    }

    @Override
    public String createOrder(ServiceOrder order) {
        var response = client.post().uri("/service-orders")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .body(FieldServiceOrderRequest.from(order))
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), (request, result) -> {
                    throw new IllegalStateException("Field Service returned HTTP " + result.getStatusCode().value());
                })
                .body(CreateOrderResponse.class);
        if (response == null || response.externalOrderId() == null || response.externalOrderId().isBlank()) {
            throw new IllegalStateException("Field Service returned no external order identifier");
        }
        return response.externalOrderId();
    }

    public record CreateOrderResponse(String externalOrderId) { }
}
