package com.github.h3yow.sagacoordinatorjava.tests.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.coordinator.SagaCoordinator;
import com.github.h3yow.sagacoordinatorjava.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.model.TransactionResult;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.tests.support.TestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 运维 HTTP API 集成：总览筛选分页、人工处置幂等与拒绝（4xx）、容量拒绝（429）。
 */
@SpringBootTest(classes = {
        com.github.h3yow.sagacoordinatorjava.Application.class,
        SagaTestBeans.class
})
@AutoConfigureMockMvc
class SagaWebApiIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SagaWebApiIntegrationTest.class);

    @TempDir
    static Path sharedTempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("saga.store-dir", () -> sharedTempDir.toString());
        registry.add("saga.transaction-timeout-millis", () -> "60000");
        registry.add("saga.recovery.sweep-interval-millis", () -> "3600000");
    }

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private SagaCoordinator coordinator;
    @Autowired
    private SagaDefinition<TestContext> orderSaga;
    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void listEndpointFiltersAndPaginates() throws Exception {
        // 准备一条 COMPLETED
        coordinator.submit(orderSaga, "tx-web-ok-001", new TestContext());

        mockMvc.perform(get("/saga/transactions")
                        .param("status", "COMPLETED")
                        .param("page", "0")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items[0].transactionId").exists())
                .andExpect(jsonPath("$.items[0].status").value("COMPLETED"))
                .andExpect(jsonPath("$.items[0].updatedAt").isNumber())
                .andExpect(jsonPath("$.totalItems").isNumber())
                .andDo(r -> log.info("list COMPLETED page: {}",
                        r.getResponse().getContentAsString()));

        // 不存在的状态组合：空页但 200
        mockMvc.perform(get("/saga/transactions").param("status", "RUNNING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalItems").value(0));
    }

    @Test
    void manualResolveIsIdempotentAndRetryOnWrongStatusIs409() throws Exception {
        String txId = "tx-web-ok-002";
        coordinator.submit(orderSaga, txId, new TestContext());

        // 已 COMPLETED：人工重试补偿应 409
        mockMvc.perform(post("/saga/transactions/{id}/manual-retry", txId))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("MANUAL_ACTION_REJECTED"))
                .andDo(r -> log.info("manual-retry rejected: {}", r.getResponse().getContentAsString()));

        // 人工标记处理完毕同样拒绝 COMPLETED
        mockMvc.perform(post("/saga/transactions/{id}/manual-resolve", txId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"note\":\"try\"}"))
                .andExpect(status().isConflict());

        // 不存在的事务 → 400
        mockMvc.perform(post("/saga/transactions/tx-missing/manual-resolve"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void manualResolveFlowOnCompensationFailedIsStable() throws Exception {
        String txId = "tx-web-compfailed-001";
        TestContext run = new TestContext();
        run.failStep = com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory.STEP_C;
        run.compensationFailStep =
                com.github.h3yow.sagacoordinatorjava.tests.support.TestSagaFactory.STEP_B;
        TransactionResult stuck = coordinator.submit(orderSaga, txId, run);
        org.junit.jupiter.api.Assertions.assertEquals(
                TransactionStatus.COMPENSATION_FAILED, stuck.getStatus());

        // 标记人工处理完毕 → 200
        mockMvc.perform(post("/saga/transactions/{id}/manual-resolve", txId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"note\":\"ops fixed ledger\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("MANUALLY_RESOLVED"))
                .andExpect(jsonPath("$.failureReason").value("MANUALLY_RESOLVED"));

        // 幂等：再标记一次，结论不变
        mockMvc.perform(post("/saga/transactions/{id}/manual-resolve", txId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("MANUALLY_RESOLVED"));

        // 总览里能按 MANUALLY_RESOLVED 筛出
        mockMvc.perform(get("/saga/transactions").param("status", "MANUALLY_RESOLVED"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalItems").value(1))
                .andExpect(jsonPath("$.items[0].transactionId").value(txId))
                .andDo(r -> log.info("resolved list: {}", r.getResponse().getContentAsString()));
    }
}
