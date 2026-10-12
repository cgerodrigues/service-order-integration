package io.github.cgerodrigues.serviceorderintegration.domain.serviceorder;

import java.time.OffsetDateTime;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceOrderTests {

	private static ValidatorFactory validatorFactory;
	private static Validator validator;

	@BeforeAll
	static void createValidator() {
		validatorFactory = Validation.buildDefaultValidatorFactory();
		validator = validatorFactory.getValidator();
	}

	@AfterAll
	static void closeValidatorFactory() {
		validatorFactory.close();
	}

	@Test
	void fullyValidOrderPassesValidation() {
		assertThat(validator.validate(validOrder())).isEmpty();
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "\t\n" })
	void rejectsInvalidRequiredText(String value) {
		ServiceOrder valid = validOrder();
		ServiceOrder order = new ServiceOrder(value, value, valid.type(), null,
				new Customer(value, value),
				new ServiceAddress(value, value, null, null, value, value, value, value),
				valid.priority(), null, null);

		assertViolationPaths(order, "id", "name", "customer.id", "customer.name",
				"serviceAddress.street", "serviceAddress.number", "serviceAddress.city",
				"serviceAddress.state", "serviceAddress.postalCode", "serviceAddress.country");
	}

	@Test
	void rejectsBlankOrderId() {
		ServiceOrder valid = validOrder();
		ServiceOrder order = new ServiceOrder(" ", valid.name(), valid.type(), null,
				valid.customer(), valid.serviceAddress(), valid.priority(), null, null);

		assertViolationPaths(order, "id");
	}

	@Test
	void rejectsMissingCustomer() {
		ServiceOrder valid = validOrder();
		ServiceOrder order = new ServiceOrder(valid.id(), valid.name(), valid.type(), null,
				null, valid.serviceAddress(), valid.priority(), null, null);

		assertViolationPaths(order, "customer");
	}

	@Test
	void rejectsOtherMissingRequiredValues() {
		ServiceOrder order = new ServiceOrder("order-1", "Install connection", null, null,
				new Customer("customer-1", "Alex Smith"), null, null, null, null);

		assertViolationPaths(order, "type", "serviceAddress", "priority");
	}

	@Test
	void validatesNestedCustomer() {
		ServiceOrder valid = validOrder();
		ServiceOrder order = new ServiceOrder(valid.id(), valid.name(), valid.type(), null,
				new Customer("customer-1", " "), valid.serviceAddress(), valid.priority(), null, null);

		assertViolationPaths(order, "customer.name");
	}

	@Test
	void validatesNestedServiceAddress() {
		ServiceOrder valid = validOrder();
		ServiceAddress address = new ServiceAddress("Main Street", "10", null, null,
				" ", "CE", "60000-000", "BR");
		ServiceOrder order = new ServiceOrder(valid.id(), valid.name(), valid.type(), null,
				valid.customer(), address, valid.priority(), null, null);

		assertViolationPaths(order, "serviceAddress.city");
	}

	@Test
	void validatesNestedRequestedWindow() {
		ServiceOrder valid = validOrder();
		ServiceOrder order = new ServiceOrder(valid.id(), valid.name(), valid.type(), null,
				valid.customer(), valid.serviceAddress(), valid.priority(),
				new RequestedWindow(null, null), null);

		assertViolationPaths(order, "requestedWindow.start", "requestedWindow.end");
	}

	@Test
	void acceptsAbsentOptionalValues() {
		ServiceOrder valid = validOrder();
		ServiceAddress address = new ServiceAddress("Main Street", "10", null, null,
				"Fortaleza", "CE", "60000-000", "BR");
		ServiceOrder order = new ServiceOrder(valid.id(), valid.name(), valid.type(), null,
				valid.customer(), address, valid.priority(), null, null);

		assertThat(validator.validate(order)).isEmpty();
	}

	private static void assertViolationPaths(ServiceOrder order, String... paths) {
		assertThat(validator.validate(order))
				.extracting(violation -> violation.getPropertyPath().toString())
				.containsExactlyInAnyOrder(paths);
	}

	private static ServiceOrder validOrder() {
		return new ServiceOrder("order-1", "Install connection", ServiceOrderType.INSTALLATION,
				"Install the requested connection", new Customer("customer-1", "Alex Smith"),
				new ServiceAddress("Main Street", "10", "Apartment 2", "Centro",
						"Fortaleza", "CE", "60000-000", "BR"),
				ServiceOrderPriority.NORMAL,
				new RequestedWindow(OffsetDateTime.parse("2026-10-09T09:00:00-03:00"),
						OffsetDateTime.parse("2026-10-09T11:00:00-03:00")),
				"Call on arrival");
	}

}
