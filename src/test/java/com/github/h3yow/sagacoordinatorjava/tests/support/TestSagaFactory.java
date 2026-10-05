package com.github.h3yow.sagacoordinatorjava.tests.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.coordinator.TransactionLockManager;
import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.SagaRegistry;
import com.github.h3yow.sagacoordinatorjava.store.FileTransactionStore;

import java.nio.file.Path;

/**
 * 测试夹具：在临时目录上构建协调器与标准三步 Saga。
 */
public final class TestSagaFactory {

    public static final String SAGA_TYPE = "order-saga";
    public static final String STEP_A = "reserve-inventory";
    public static final String STEP_B = "charge-payment";
    public static final String STEP_C = "create-shipment";

    private TestSagaFactory() {
    }

    public static SagaDefinition<TestContext> threeStepDefinition() {
        SagaDefinition<TestContext> def = new SagaDefinition<>(SAGA_TYPE);
        def.step(STEP_A,
                ctx -> ctx.execute(STEP_A),
                ctx -> ctx.compensate(STEP_A))
           .step(STEP_B,
                ctx -> ctx.execute(STEP_B),
                ctx -> ctx.compensate(STEP_B))
           .step(STEP_C,
                ctx -> ctx.execute(STEP_C),
                ctx -> ctx.compensate(STEP_C));
        return def;
    }

    public static SagaCoordinator newCoordinator(Path storeDir,
                                                  long stepTimeoutMillis,
                                                  long txTimeoutMillis,
                                                  int compensationMaxRetries) {
        FileTransactionStore store = new FileTransactionStore(storeDir, new ObjectMapper());
        SagaRegistry registry = new SagaRegistry();
        return new SagaCoordinator(store, registry, new TransactionLockManager(),
                stepTimeoutMillis, txTimeoutMillis, compensationMaxRetries);
    }

    public static SagaCoordinator newCoordinator(Path storeDir,
                                                  long stepTimeoutMillis,
                                                  long txTimeoutMillis,
                                                  int compensationMaxRetries,
                                                  int maxConcurrentTransactions,
                                                  long admissionQueueTimeoutMillis) {
        FileTransactionStore store = new FileTransactionStore(storeDir, new ObjectMapper());
        SagaRegistry registry = new SagaRegistry();
        return new SagaCoordinator(store, registry, new TransactionLockManager(),
                stepTimeoutMillis, txTimeoutMillis, compensationMaxRetries,
                new com.github.h3yow.sagacoordinatorjava.coordinator.AdmissionController(
                        maxConcurrentTransactions, admissionQueueTimeoutMillis));
    }
}
