package io.github.cgerodrigues.serviceorderintegration.infrastructure.kafka;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.EventMetadata;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizationEventPort;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;

@Component
public final class KafkaSynchronizationEventPublisher implements SynchronizationEventPort {
    private final KafkaTemplate<String, String> kafka;
    private final ServiceOrderCloudEvents events;
    private final String topic;

    public KafkaSynchronizationEventPublisher(KafkaTemplate<String, String> kafka,
            ServiceOrderCloudEvents events, @Value("${integration.kafka.success-topic}") String topic) {
        this.kafka = kafka;
        this.events = events;
        this.topic = topic;
    }

    @Override
    public void publish(String serviceOrderId, String externalOrderId, EventMetadata metadata) {
        var record = new ProducerRecord<String, String>(topic, serviceOrderId,
                events.writeSynchronized(serviceOrderId, externalOrderId, metadata));
        record.headers().add("content-type", ServiceOrderCloudEvents.CONTENT_TYPE.getBytes(StandardCharsets.UTF_8));
        try {
            // Do not acknowledge the consumed record until the broker confirms publication.
            kafka.send(record).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing synchronization event", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("Synchronization event publication failed", ex);
        }
    }
}
