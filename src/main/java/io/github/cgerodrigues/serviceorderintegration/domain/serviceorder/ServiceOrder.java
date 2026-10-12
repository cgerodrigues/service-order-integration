package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record ServiceOrder(
		@NotBlank String id,
		@NotBlank String name,
		@NotNull ServiceOrderType type,
		String description,
		@NotNull @Valid Customer customer,
		@NotNull @Valid ServiceAddress serviceAddress,
		@NotNull ServiceOrderPriority priority,
		@Valid RequestedWindow requestedWindow,
		String notes) {
}
