package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RequestedWindowTests {

	private static final OffsetDateTime START = OffsetDateTime.parse("2026-10-09T09:00:00-03:00");

	@Test
	void acceptsValidRange() {
		OffsetDateTime end = START.plusHours(2);

		RequestedWindow window = new RequestedWindow(START, end);

		assertThat(window.start()).isEqualTo(START);
		assertThat(window.end()).isEqualTo(end);
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"2026-10-09T09:00:00-03:00",
			"2026-10-09T08:59:59-03:00",
			"2026-10-09T12:00:00Z",
			"2026-10-09T10:00:00Z"
	})
	void rejectsEndAtOrBeforeStart(String end) {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RequestedWindow(START, OffsetDateTime.parse(end)))
				.withMessage("Requested window end must be strictly after start");
	}

	@Test
	void acceptsLaterInstantWithEarlierLocalTime() {
		OffsetDateTime end = OffsetDateTime.parse("2026-10-09T08:30:00-04:00");

		RequestedWindow window = new RequestedWindow(START, end);

		assertThat(window.end()).isEqualTo(end);
	}

}
