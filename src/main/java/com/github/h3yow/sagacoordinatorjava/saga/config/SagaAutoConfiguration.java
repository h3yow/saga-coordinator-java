package com.github.h3yow.sagacoordinatorjava.saga.config;

import com.github.h3yow.sagacoordinatorjava.saga.engine.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.saga.engine.StepRegistry;
import com.github.h3yow.sagacoordinatorjava.saga.store.FileEventLog;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;

/**
 * Spring 装配：事件日志、步骤注册表、协调器单例。
 */
@Configuration
@EnableConfigurationProperties(SagaProperties.class)
public class SagaAutoConfiguration {

    private SagaCoordinator coordinator;

    @Bean
    public StepRegistry stepRegistry() {
        return new StepRegistry();
    }

    @Bean
    public SagaCoordinator sagaCoordinator(SagaProperties properties, StepRegistry registry) {
        this.coordinator = new SagaCoordinator(
                new FileEventLog(properties.getStoreDir()), registry);
        return coordinator;
    }

    /** 进程启动后重放事件日志，继续推进未完成事务并启动超时巡检。 */
    @Bean
    public ApplicationRunner sagaRecoveryRunner(SagaCoordinator coordinator) {
        return (ApplicationArguments args) -> coordinator.recoverAfterRestart();
    }

    @PreDestroy
    public void shutdown() {
        if (coordinator != null) {
            coordinator.shutdown();
        }
    }
}
