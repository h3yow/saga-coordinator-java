package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaContext;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaException;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaStep;
import com.github.h3yow.sagacoordinatorjava.saga.model.StepState;
import com.github.h3yow.sagacoordinatorjava.saga.store.EventLog;
import com.github.h3yow.sagacoordinatorjava.saga.store.EventType;
import com.github.h3yow.sagacoordinatorjava.saga.store.SagaEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 把事件日志重放为 {@link SagaInstance} 聚合。
 *
 * <p>重放是崩溃恢复的基础：内存中不保存任何无法重建的状态。重放结束后，
 * 崩溃前停在 RUNNING 的步骤按“结果未知”处理（见引擎的超时收敛规则）。
 */
public class SagaReplayer {

    private static final Logger log = LoggerFactory.getLogger(SagaReplayer.class);

    private final EventLog eventLog;
    private final StepRegistry registry;
    private final ObjectMapper mapper = new ObjectMapper();

    public SagaReplayer(EventLog eventLog, StepRegistry registry) {
        this.eventLog = eventLog;
        this.registry = registry;
    }

    /**
     * 重放指定事务。definitionHint 仅用于新实例；恢复时定义来自 SAGA_CREATED。
     */
    public SagaInstance replay(String sagaId, SagaDefinition definitionHint) {
        List<SagaEvent> events = eventLog.read(sagaId);
        if (events.isEmpty()) {
            if (definitionHint == null) {
                throw new SagaException("无事件日志且未提供定义，无法重放: " + sagaId);
            }
            return new SagaInstance(sagaId, definitionHint, new SagaContext(sagaId));
        }

        SagaEvent created = events.get(0);
        if (created.type() != EventType.SAGA_CREATED) {
            throw new SagaException("事件日志首事件不是 SAGA_CREATED: " + sagaId);
        }
        DefinitionEnvelope envelope;
        try {
            envelope = mapper.readValue(created.detail(), DefinitionEnvelope.class);
        } catch (Exception e) {
            throw new SagaException("SAGA_CREATED 元数据损坏: " + sagaId, e);
        }

        List<SagaStep> steps = new ArrayList<>();
        boolean missing = false;
        for (String name : envelope.stepNames()) {
            SagaStep step = registry.find(name).orElse(null);
            if (step == null) {
                missing = true;
            }
            steps.add(step);
        }

        SagaDefinition definition = new SagaDefinition(
                envelope.type(), steps,
                Duration.ofMillis(envelope.stepTimeoutMillis()),
                Duration.ofMillis(envelope.sagaTimeoutMillis()),
                envelope.compensationRetries());
        SagaInstance instance = new SagaInstance(sagaId, definition, new SagaContext(sagaId));
        replayInto(instance, events);

        if (missing && !instance.isTerminal()) {
            // 参与者实现缺失时不能静默丢弃：标记为待人工处理并给出原因
            instance.setReason(FailureReason.STEP_UNKNOWN);
            List<String> absent = envelope.stepNames().stream()
                    .filter(n -> registry.find(n).isEmpty())
                    .toList();
            instance.setReasonDetail("重启后缺失参与者实现: " + absent);
            if (instance.getState() == SagaState.COMPENSATING
                    || instance.getState() == SagaState.RUNNING
                    || instance.getState() == SagaState.STARTED) {
                instance.setState(SagaState.MANUAL_INTERVENTION);
            }
            log.warn("[{}] 状态迁移重放: -> MANUAL_INTERVENTION, 原因=STEP_UNKNOWN, 详情={}",
                    sagaId, absent);
        }
        return instance;
    }

    private void replayInto(SagaInstance instance, List<SagaEvent> events) {
        for (SagaEvent event : events) {
            applyEvent(instance, event);
            log.debug("[{}] 重放事件: seq={}, type={}, step={}",
                    instance.getSagaId(), event.seq(), event.type(), event.stepIndex());
        }
    }

    private void applyEvent(SagaInstance instance, SagaEvent event) {
        List<StepStateRecord> records = instance.getStepRecords();
        switch (event.type()) {
            case SAGA_CREATED -> {
                instance.setState(SagaState.STARTED);
                instance.setCurrentStepIndex(0);
            }
            case SAGA_COMPENSATION_STARTED -> {
                instance.setState(SagaState.COMPENSATING);
                int anchor = event.stepIndex() == null ? 0 : event.stepIndex();
                instance.setCompensationFromIndex(anchor);
                instance.setCurrentStepIndex(anchor);
            }
            case STEP_STARTED -> {
                StepStateRecord r = records.get(event.stepIndex());
                r.setState(StepState.RUNNING);
                r.incAttempts();
                r.setLastMessage(event.detail());
                instance.setCurrentStepIndex(event.stepIndex());
                if (instance.getState() == SagaState.STARTED) {
                    instance.setState(SagaState.RUNNING);
                }
            }
            case STEP_COMPLETED -> {
                records.get(event.stepIndex()).setState(StepState.COMPLETED);
                instance.setCurrentStepIndex(event.stepIndex() + 1);
            }
            case STEP_FAILED -> {
                records.get(event.stepIndex()).setState(StepState.FAILED);
                instance.setReason(FailureReason.STEP_FAILED);
                instance.setReasonDetail(event.detail());
            }
            case STEP_TIMED_OUT -> {
                records.get(event.stepIndex()).setState(StepState.FAILED);
                instance.setReason(FailureReason.STEP_TIMEOUT);
                instance.setReasonDetail(event.detail());
            }
            case STEP_COMPENSATION_STARTED -> {
                StepStateRecord r = records.get(event.stepIndex());
                r.setState(StepState.COMPENSATING);
                r.incCompensationAttempts();
                // 首个补偿事件揭示补偿起点（覆盖失败步骤之后所有已完成步骤）
                if (instance.getState() != SagaState.COMPENSATING) {
                    instance.setState(SagaState.COMPENSATING);
                    int anchor = 0;
                    for (int k = 0; k < records.size(); k++) {
                        StepState rs = records.get(k).getState();
                        if (rs != StepState.PENDING && rs != StepState.RUNNING) {
                            anchor = Math.max(anchor, k);
                        }
                    }
                    instance.setCompensationFromIndex(anchor);
                }
            }
            case STEP_COMPENSATED -> records.get(event.stepIndex())
                    .setState(StepState.COMPENSATED);
            case STEP_COMPENSATION_FAILED -> {
                records.get(event.stepIndex()).setState(StepState.COMPENSATION_FAILED);
                instance.setReason(FailureReason.COMPENSATION_FAILED);
                instance.setReasonDetail(event.detail());
            }
            case SAGA_FINISHED -> {
                SagaState target = SagaState.valueOf(event.detail());
                instance.setState(target);
                if (event.reason() != null && event.reason() != FailureReason.NONE) {
                    instance.setReason(event.reason());
                }
            }
        }
    }

    /** 启动时从日志目录加载全部事务。 */
    public List<SagaInstance> recoverAll() {
        List<SagaInstance> all = new ArrayList<>();
        for (String sagaId : eventLog.sagaIds()) {
            all.add(replay(sagaId, null));
        }
        return all;
    }
}
