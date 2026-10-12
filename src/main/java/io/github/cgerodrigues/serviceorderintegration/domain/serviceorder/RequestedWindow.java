package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import java.time.OffsetDateTime;

import jakarta.validation.constraints.NotNull;

public record RequestedWindow(
		@NotNull OffsetDateTime start,
		@NotNull OffsetDateTime end) {

	public RequestedWindow {
		if (start != null && end != null && !end.isAfter(start)) {
			throw new IllegalArgumentException("Requested window end must be strictly after start");
		}
	}

}
