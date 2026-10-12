package io.github.cgerodrigues.serviceorderintegration.support;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import tools.jackson.databind.json.JsonMapper;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;

public final class EventFixtures {
    public static final JsonMapper MAPPER = JsonMapper.builder().findAndAddModules().build();
    public static final ServiceOrderCloudEvents EVENTS = new ServiceOrderCloudEvents(MAPPER,
            Clock.fixed(Instant.parse("2026-10-12T12:00:00Z"), ZoneOffset.UTC));

    public static String created() {
        try (var stream = EventFixtures.class.getResourceAsStream("/events/service-order-created.json")) {
            if (stream == null) throw new IllegalStateException("Missing event fixture");
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
