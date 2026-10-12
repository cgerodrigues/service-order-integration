package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import jakarta.validation.constraints.NotBlank;

public record ServiceAddress(
		@NotBlank String street,
		@NotBlank String number,
		String complement,
		String district,
		@NotBlank String city,
		@NotBlank String state,
		@NotBlank String postalCode,
		@NotBlank String country) {
}
