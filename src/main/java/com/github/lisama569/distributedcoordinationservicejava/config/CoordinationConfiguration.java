package com.github.lisama569.distributedcoordinationservicejava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 协调模拟服务的 Spring 配置入口。
 */
@Configuration
@EnableConfigurationProperties(CoordinationProperties.class)
public class CoordinationConfiguration {
}
