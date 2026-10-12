package io.github.cgerodrigues.serviceorderintegration.application.serviceorder;

public interface SynchronizationEventPort {
    void publish(String serviceOrderId, String externalOrderId, EventMetadata metadata);
}
