package io.github.cgerodrigues.serviceorderintegration.application.serviceorder;

import org.junit.jupiter.api.Test;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static io.github.cgerodrigues.serviceorderintegration.support.EventFixtures.*;

class SynchronizeServiceOrderTests {
    private final FieldServicePort fieldService = mock(FieldServicePort.class);
    private final SynchronizationEventPort publisher = mock(SynchronizationEventPort.class);
    private final SynchronizeServiceOrder useCase = new SynchronizeServiceOrder(fieldService, publisher);

    @Test
    void deliversBeforePublishingSuccess() {
        var event = EVENTS.readCreated(created());
        when(fieldService.createOrder(event.order())).thenReturn("external-1");
        useCase.synchronize(event.order(), event.metadata());
        var sequence = inOrder(fieldService, publisher);
        sequence.verify(fieldService).createOrder(event.order());
        sequence.verify(publisher).publish("order-1", "external-1", event.metadata());
    }

    @Test
    void doesNotPublishAfterDeliveryFailure() {
        var event = EVENTS.readCreated(created());
        when(fieldService.createOrder(any())).thenThrow(new IllegalStateException("unavailable"));
        assertThatThrownBy(() -> useCase.synchronize(event.order(), event.metadata())).hasMessage("unavailable");
        verifyNoInteractions(publisher);
    }

    @Test
    void rejectsEmptyExternalIdentifier() {
        var event = EVENTS.readCreated(created());
        when(fieldService.createOrder(any())).thenReturn(" ");
        assertThatThrownBy(() -> useCase.synchronize(event.order(), event.metadata())).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(publisher);
    }

    @Test
    void propagatesPublicationFailure() {
        var event = EVENTS.readCreated(created());
        when(fieldService.createOrder(any())).thenReturn("external-1");
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(any(), any(), any());
        assertThatThrownBy(() -> useCase.synchronize(event.order(), event.metadata())).hasMessage("broker unavailable");
    }
}
