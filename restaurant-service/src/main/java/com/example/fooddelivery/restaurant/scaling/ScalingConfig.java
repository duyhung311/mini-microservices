package com.example.fooddelivery.restaurant.scaling;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration
public class ScalingConfig {

    @Bean
    public InstanceId instanceId(@Value("${app.instance-id:}") String configured) {
        return InstanceId.resolve(configured);
    }

    @Bean
    public FilterRegistrationBean<InstanceIdFilter> instanceIdFilter(InstanceId instanceId) {
        FilterRegistrationBean<InstanceIdFilter> reg = new FilterRegistrationBean<>(new InstanceIdFilter(instanceId));
        reg.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return reg;
    }
}
