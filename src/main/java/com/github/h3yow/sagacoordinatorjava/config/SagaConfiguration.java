package com.github.h3yow.sagacoordinatorjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.coordinator.AdmissionController;
import com.github.h3yow.sagacoordinatorjava.coordinator.RecoveryService;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionLockManager;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionQueryService;
import com.github.h3yow.sagacoordinatorjava.saga.SagaRegistry;
import com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore;
import com.github.h3yow.sagacoordinatorjava.store.TransactionStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.nio.file.Path;

/**
 * 协调器各组件的 Spring 装配。
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(SagaProperties.class)
public class SagaConfiguration {

    @Bean
    public TransactionStore transactionStore(SagaProperties properties, ObjectMapper objectMapper) {
        return new FileTransactionStore(Path.of(properties.getStoreDir()), objectMapper);
    }

    @Bean
    public SagaRegistry sagaRegistry() {
        return new SagaRegistry();
    }

    @Bean
    public TransactionLockManager transactionLockManager() {
        return new TransactionLockManager();
    }

    @Bean
    public AdmissionController admissionController(SagaProperties properties) {
        return new AdmissionController(properties.getMaxConcurrentTransactions(),
                properties.getAdmissionQueueTimeoutMillis());
    }

    @Bean
    public SagaCoordinator sagaCoordinator(TransactionStore store, SagaRegistry registry,
                                           TransactionLockManager lockManager,
                                           AdmissionController admissionController,
                                           SagaProperties properties) {
        return new SagaCoordinator(store, registry, lockManager,
                properties.getStepTimeoutMillis(),
                properties.getTransactionTimeoutMillis(),
                properties.getCompensationMaxRetries(),
                admissionController);
    }

    @Bean
    public TransactionQueryService transactionQueryService(TransactionStore store) {
        return new TransactionQueryService(store);
    }

    @Bean
    public RecoveryService recoveryService(TransactionStore store, SagaCoordinator coordinator) {
        return new RecoveryService(store, coordinator);
    }
}
