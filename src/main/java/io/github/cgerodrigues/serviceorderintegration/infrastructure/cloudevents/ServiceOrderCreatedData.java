package io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents;

import java.time.OffsetDateTime;

/** Payload schema for com.cgerodrigues.serviceorder.created.v1. */
public record ServiceOrderCreatedData(String id, String name, Type type, String description,
        CustomerData customer, ServiceAddressData serviceAddress, Priority priority,
        RequestedWindowData requestedWindow, String notes) {

    public record CustomerData(String id, String name) { }

    public record ServiceAddressData(String street, String number, String complement, String district,
                                    String city, String state, String postalCode, String country) { }

    public record RequestedWindowData(OffsetDateTime start, OffsetDateTime end) { }

    public enum Type { INSTALLATION, MAINTENANCE, REPAIR, INSPECTION }

    public enum Priority { LOW, NORMAL, HIGH, URGENT }
}
