package com.github.h3yow.sagacoordinatorjava.tests.spring;

import com.github.h3yow.sagacoordinatorjava.coordinator.RecoveryService;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 集成测试：Spring 装配 + 启动时自动恢复磁盘上的非终态事务。
 * 先在"进程外"（独立协调器）落盘一个已完成事务，再启动上下文验证结论可读；
 * 同时验证健康事务在超时巡检下不受影响。
 */
@SpringBootTest(classes = {
        com.github.h3yow.sagacoordinatorjava.Application.class,
        SagaTestBeans.class
})
class SpringRecoveryIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SpringRecoveryIntegrationTest.class);

    @TempDir
    static Path sharedTempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("saga.store-dir", () -> sharedTempDir.toString());
        registry.add("saga.transaction-timeout-millis", () -> "60000");
        registry.add("saga.recovery.sweep-interval-millis", () -> "3600000");
    }

    @Autowired
    private SagaCoordinator coordinator;

    @Autowired
    private RecoveryService recoveryService;

    @Autowired
    private SagaDefinition<TestContext> orderSaga;

    @Test
    void contextWiresBeansAndPersistedTransactionIsReadable() {
        String txId = "tx-spring-001";
        TransactionResult result = coordinator.submit(orderSaga, txId, new TestContext());
        log.info("tx={} status={} (via Spring beans)", txId, result.getStatus());
        result.getTrace().forEach(e -> log.info("tx={} trace {}", txId, e));

        assertEquals(TransactionStatus.COMPLETED, result.getStatus());

        // 手动触发一次全量恢复，终态事务不应被改动、也不应报错
        recoveryService.recoverAll();
        TransactionResult reread = coordinator.getResult(txId).orElseThrow();
        assertEquals(TransactionStatus.COMPLETED, reread.getStatus());
    }

    @Test
    void multipleTransactionsAreIsolatedDuringSweep() {
        String tx1 = "tx-spring-multi-001";
        String tx2 = "tx-spring-multi-002";
        coordinator.submit(orderSaga, tx1, new TestContext());
        coordinator.submit(orderSaga, tx2, new TestContext());

        recoveryService.sweepTimeouts(System.currentTimeMillis());

        assertEquals(TransactionStatus.COMPLETED,
                coordinator.getResult(tx1).orElseThrow().getStatus());
        assertEquals(TransactionStatus.COMPLETED,
                coordinator.getResult(tx2).orElseThrow().getStatus());
        log.info("tx={} and tx={} both COMPLETED after sweep", tx1, tx2);
    }
}
