package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 超时巡检器。
 *
 * <p>周期性扫描所有非终结事务：
 * <ol>
 *   <li>事务总存活时间超过 sagaTimeout —— 按 SAGA_TIMEOUT 触发逆序收敛；</li>
 *   <li>某步骤长时间（staleThreshold）无进展（参与者线程挂死，或进程在
 *       步骤中途崩溃后重启）—— 按 STEP_TIMEOUT 触发逆序补偿；</li>
 *   <li>补偿长期无法收敛的事务标记为终态，绝不静默丢弃。</li>
 * </ol>
 * 因为引擎在锁外调用参与者，被挂死的驱动线程不持有事务锁，巡检器可以
 * 正常拿锁接管。每个事务独立处理，单个事务异常不阻塞其它事务。
 */
public class TimeoutReaper {

    private static final Logger log = LoggerFactory.getLogger(TimeoutReaper.class);

    private final SagaEngine engine;
    private final SagaRepository repository;
    private final SagaCoordinator coordinator;
    private final Duration interval;
    private final Duration staleThreshold;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TimeoutReaper(SagaEngine engine, SagaRepository repository,
                         SagaCoordinator coordinator,
                         Duration interval, Duration staleThreshold) {
        this.engine = engine;
        this.repository = repository;
        this.coordinator = coordinator;
        this.interval = interval;
        this.staleThreshold = staleThreshold;
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "saga-reaper");
            t.setDaemon(true);
            return t;
        };
        this.scheduler = Executors.newScheduledThreadPool(1, tf);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(this::sweep,
                    interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
            log.info("超时巡检器已启动: interval={}, staleThreshold={}", interval, staleThreshold);
        }
    }

    public void stop() {
        running.set(false);
        scheduler.shutdownNow();
    }

    /** 执行一轮扫描（也可供测试直接调用）。 */
    public void sweep() {
        for (SagaInstance instance : repository.findAll()) {
            String sagaId = instance.getSagaId();
            try {
                sweepOne(sagaId, instance);
            } catch (Exception e) {
                // 单事务处理失败不能阻塞其它事务
                log.error("[{}] 超时巡检处理异常，已跳过该事务: {}", sagaId, e.toString(), e);
            }
        }
    }

    private void sweepOne(String sagaId, SagaInstance instance) {
        Object lock = coordinator.lockFor(sagaId);
        FailureReason decision;
        String detail;
        synchronized (lock) {
            if (instance.isTerminal()) {
                return;
            }
            long idle = System.currentTimeMillis() - instance.getUpdatedAt();
            long age = System.currentTimeMillis() - instance.getCreatedAt();
            long sagaTimeout = instance.getDefinition().sagaTimeout().toMillis();
            SagaState state = instance.getState();

            if (age > sagaTimeout && (state == SagaState.RUNNING
                    || state == SagaState.COMPENSATING)) {
                decision = FailureReason.SAGA_TIMEOUT;
                detail = "巡检器判定事务总超时 age=" + age + "ms";
                log.warn("[{}] 巡检: 事务总超时 age={}ms > {}ms，驱动超时收敛",
                        sagaId, age, sagaTimeout);
            } else if (state == SagaState.RUNNING && idle > staleThreshold.toMillis()) {
                decision = FailureReason.STEP_TIMEOUT;
                detail = "巡检器判定步骤长时间无进展 idle=" + idle + "ms";
                log.warn("[{}] 巡检: 事务无进展 idle={}ms > {}ms，判定步骤挂起并收敛",
                        sagaId, idle, staleThreshold.toMillis());
            } else {
                // COMPENSATING 无进展由总超时兜底；正常补偿重试很快，不在此干预
                return;
            }
        }
        // 接管动作（含参与者补偿调用）在锁外执行，不阻塞同事务查询与其它事务
        engine.convergeStuckStep(instance, lock, decision, detail);
    }
}
