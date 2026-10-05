package com.github.h3yow.sagacoordinatorjava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.coordinator.ConcurrencyLimiter;
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
    public ConcurrencyLimiter concurrencyLimiter(SagaProperties properties) {
        int max = properties.getMaxConcurrentTransactions();
        if (max <= 0) {
            return ConcurrencyLimiter.unlimited();
        }
        ConcurrencyLimiter.Policy policy = "REJECT".equalsIgnoreCase(properties.getCapacityPolicy())
                ? ConcurrencyLimiter.Policy.REJECT
                : ConcurrencyLimiter.Policy.QUEUE;
        return ConcurrencyLimiter.of(max, policy, properties.getCapacityAcquireTimeoutMillis());
    }

    @Bean
    public SagaCoordinator sagaCoordinator(TransactionStore store, SagaRegistry registry,
                                           TransactionLockManager lockManager,
                                           SagaProperties properties,
                                           ConcurrencyLimiter limiter) {
        return new SagaCoordinator(store, registry, lockManager,
                properties.getStepTimeoutMillis(),
                properties.getTransactionTimeoutMillis(),
                properties.getCompensationMaxRetries(),
                limiter);
    }

    @Bean
    public RecoveryService recoveryService(TransactionStore store, SagaCoordinator coordinator) {
        return new RecoveryService(store, coordinator);
    }

    @Bean
    public TransactionQueryService transactionQueryService(TransactionStore store) {
        return new TransactionQueryService(store);
    }
}
