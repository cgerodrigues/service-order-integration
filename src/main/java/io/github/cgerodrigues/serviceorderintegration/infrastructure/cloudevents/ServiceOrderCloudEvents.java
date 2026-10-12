package io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents;

import java.net.URI;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.springframework.http.MediaType;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.EventMetadata;
import io.github.cgerodrigues.serviceorderintegration.domain.serviceorder.ServiceOrder;

/** Structured JSON CloudEvents 1.0 contract for the two supported event types. */
public final class ServiceOrderCloudEvents {
    public static final String CREATED = "com.cgerodrigues.serviceorder.created.v1";
    public static final String SYNCHRONIZED = "com.cgerodrigues.serviceorder.synchronized.v1";
    public static final String CONTENT_TYPE = "application/cloudevents+json";
    private final JsonMapper mapper;
    private final Clock clock;

    public ServiceOrderCloudEvents(JsonMapper mapper, Clock clock) {
        this.mapper = mapper.rebuild()
                .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                .disable(DateTimeFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
                .build();
        this.clock = clock;
    }

    public CreatedEvent readCreated(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("CloudEvent body is required");
        }
        try {
            JsonNode event = mapper.readTree(json);
            if (!event.isObject() || !"1.0".equals(requiredText(event, "specversion"))) {
                throw new IllegalArgumentException("Only structured CloudEvents 1.0 are supported");
            }
            if (!CREATED.equals(requiredText(event, "type"))) {
                throw new IllegalArgumentException("Unsupported CloudEvent type");
            }
            String eventId = requiredText(event, "id");
            URI.create(requiredText(event, "source"));
            String subject = requiredText(event, "subject");
            OffsetDateTime.parse(requiredText(event, "time"));
            MediaType contentType = MediaType.parseMediaType(requiredText(event, "datacontenttype"));
            if (!MediaType.APPLICATION_JSON.isCompatibleWith(contentType)) {
                throw new IllegalArgumentException("CloudEvent data must be application/json");
            }
            String correlationId = requiredText(event, "correlationid");
            JsonNode data = event.get("data");
            if (data == null || !data.isObject() || event.has("data_base64")) {
                throw new IllegalArgumentException("CloudEvent data must be a JSON object");
            }
            ServiceOrderCreatedData payload = mapper.treeToValue(data, ServiceOrderCreatedData.class);
            ServiceOrder order = ServiceOrderCreatedMapper.toDomain(payload);
            if (!subject.equals(order.id())) {
                throw new IllegalArgumentException("CloudEvent subject must equal the service order id");
            }
            return new CreatedEvent(order, new EventMetadata(eventId, correlationId));
        } catch (JacksonException ex) {
            throw new IllegalArgumentException("Invalid ServiceOrderCreated JSON contract", ex);
        }
    }

    public String writeSynchronized(String serviceOrderId, String externalOrderId, EventMetadata metadata) {
        var event = mapper.createObjectNode();
        event.put("specversion", "1.0");
        event.put("id", UUID.randomUUID().toString());
        event.put("source", "urn:service-order-integration");
        event.put("type", SYNCHRONIZED);
        event.put("subject", serviceOrderId);
        event.put("time", OffsetDateTime.now(clock).format(java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        event.put("datacontenttype", "application/json");
        event.put("correlationid", metadata.correlationId());
        event.put("causationid", metadata.eventId());
        var data = event.putObject("data");
        data.put("serviceOrderId", serviceOrderId);
        data.put("externalOrderId", externalOrderId);
        data.put("status", "SYNCHRONIZED");
        return mapper.writeValueAsString(event);
    }

    private static String requiredText(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || !value.isString() || value.asString().isBlank()) {
            throw new IllegalArgumentException("Missing or invalid CloudEvent attribute: " + name);
        }
        return value.asString();
    }

    public record CreatedEvent(ServiceOrder order, EventMetadata metadata) {
    }
}
