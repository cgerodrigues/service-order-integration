package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import jakarta.validation.constraints.NotBlank;

public record Customer(
		@NotBlank String id,
		@NotBlank String name) {
}
