package io.github.cgerodrigues.serviceorderintegration.infrastructure.config;

import java.time.Clock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.json.JsonMapper;

import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.FieldServicePort;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizationEventPort;
import io.github.cgerodrigues.serviceorderintegration.application.serviceorder.SynchronizeServiceOrder;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.cloudevents.ServiceOrderCloudEvents;
import io.github.cgerodrigues.serviceorderintegration.infrastructure.fieldservice.HttpFieldServiceAdapter;

@Configuration(proxyBeanMethods = false)
public class IntegrationConfiguration {
    @Bean
    ServiceOrderCloudEvents serviceOrderCloudEvents(JsonMapper mapper) {
        return new ServiceOrderCloudEvents(mapper, Clock.systemUTC());
    }

    @Bean
    FieldServicePort fieldServicePort(RestClient.Builder builder,
            @Value("${integration.field-service.base-url}") String baseUrl) {
        return new HttpFieldServiceAdapter(builder.baseUrl(baseUrl).build());
    }

    @Bean
    SynchronizeServiceOrder synchronizeServiceOrder(FieldServicePort fieldService, SynchronizationEventPort events) {
        return new SynchronizeServiceOrder(fieldService, events);
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        return new CommonContainerStoppingErrorHandler();
    }
}
