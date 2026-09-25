package com.github.h3yow.sagacoordinatorjava.saga.engine;

import com.github.h3yow.sagacoordinatorjava.saga.model.SagaDefinition;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaResult;
import com.github.h3yow.sagacoordinatorjava.saga.store.EventLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 对外协调器门面。
 *
 * <p>并发控制：
 * <ul>
 *   <li>每个 sagaId 一把锁（{@link #runLocked}）。锁只保护状态迁移与事件
 *       落盘；参与者步骤在锁外执行，因此慢/挂死的参与者不会阻塞同事务的
 *       超时接管，也不会阻塞其它事务。</li>
 *   <li>{@code driving} 记录“已有驱动在进行”的事务：并发/重复提交时后到者
 *       不再次驱动，而是在锁上等待首个驱动结束后读取同一结论，保证业务
 *       效果只产生一次。</li>
 * </ul>
 */
public class SagaCoordinator {

    private static final Logger log = LoggerFactory.getLogger(SagaCoordinator.class);

    private final EventLog eventLog;
    private final StepRegistry registry;
    private final SagaRepository repository = new InMemorySagaRepository();
    private final SagaEngine engine;
    private final SagaReplayer replayer;
    private final TimeoutReaper reaper;
    private final ExecutorService recoveryExecutors;
    private final Map<String, Object> locks = new ConcurrentHashMap<>();
    private final Set<String> driving = ConcurrentHashMap.newKeySet();

    public SagaCoordinator(EventLog eventLog, StepRegistry registry) {
        this(eventLog, registry, Duration.ofSeconds(2), Duration.ofSeconds(10));
    }

    public SagaCoordinator(EventLog eventLog, StepRegistry registry,
                           Duration reaperInterval, Duration staleThreshold) {
        this.eventLog = eventLog;
        this.registry = registry;
        this.engine = new SagaEngine(registry, eventLog);
        this.replayer = new SagaReplayer(eventLog, registry);
        this.reaper = new TimeoutReaper(engine, repository, this,
                reaperInterval, staleThreshold);
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "saga-recovery-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.recoveryExecutors = Executors.newCachedThreadPool(tf);
    }

    /** 返回该事务的独占锁。 */
    public Object lockFor(String sagaId) {
        return locks.computeIfAbsent(sagaId, k -> new Object());
    }

    /** 在指定事务的互斥锁内执行动作。 */
    public void runLocked(String sagaId, Runnable action) {
        synchronized (lockFor(sagaId)) {
            action.run();
        }
    }

    /**
     * 提交事务并驱动至结论。同一 sagaId 的重复/并发调用只产生一次业务
     * 效果：若已有驱动在进行，调用方等待其结束并读取同一结论。
     */
    public SagaResult submit(String sagaId, SagaDefinition definition) {
        Object lock = lockFor(sagaId);
        SagaInstance instance;
        boolean owner;
        synchronized (lock) {
            instance = loadOrCreate(sagaId, definition);
            repository.save(instance);
            owner = driving.add(sagaId);
            if (!owner && instance.isTerminal()) {
                return toResult(instance);
            }
        }
        try {
            if (owner) {
                engine.drive(instance, lock);
            } else {
                awaitSettled(sagaId, instance, lock);
            }
        } finally {
            if (owner) {
                driving.remove(sagaId);
            }
        }
        synchronized (lock) {
            return toResult(repository.find(sagaId).orElse(instance));
        }
    }

    /** 非属主线程：等待正在进行的驱动把事务推进到终结态。 */
    private void awaitSettled(String sagaId, SagaInstance instance, Object lock) {
        synchronized (lock) {
            while (!instance.isTerminal()) {
                try {
                    // 驱动在锁外执行参与者，锁空闲即可进入等待并在其回锁/结束时被唤醒
                    lock.wait(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        log.debug("[{}] 并发提交复用首个驱动的结论", sagaId);
    }

    private SagaInstance loadOrCreate(String sagaId, SagaDefinition definition) {
        SagaInstance existing = repository.find(sagaId).orElse(null);
        if (existing != null) {
            if (!existing.getDefinition().type().equals(definition.type())) {
                log.warn("[{}] 重复提交使用了不同事务类型，忽略新定义并返回既有结论", sagaId);
            }
            return existing;
        }
        // 内存没有（可能是新进程尚未 recover）：以日志为准重放；日志为空才新建
        if (eventLog.read(sagaId).isEmpty()) {
            return engine.create(sagaId, definition);
        }
        SagaInstance replayed = replayer.replay(sagaId, definition);
        log.info("[{}] 从事件日志恢复既有事务，当前状态={}", sagaId, replayed.getState());
        return replayed;
    }

    public SagaResult getStatus(String sagaId) {
        synchronized (lockFor(sagaId)) {
            SagaInstance instance = repository.find(sagaId).orElse(null);
            if (instance == null && !eventLog.read(sagaId).isEmpty()) {
                instance = replayer.replay(sagaId, null);
            }
            return instance == null ? null : toResult(instance);
        }
    }

    /**
     * 进程重启后调用：重放全部事件日志；未完成事务各自独立异步继续推进，
     * 单个事务卡住不阻塞其它事务。随后启动超时巡检。
     */
    public void recoverAfterRestart() {
        List<SagaInstance> recovered = replayer.recoverAll();
        for (SagaInstance instance : recovered) {
            repository.save(instance);
            String sagaId = instance.getSagaId();
            if (instance.isTerminal()) {
                log.info("[{}] 重启恢复: 事务已终结，状态={}, reason={}",
                        sagaId, instance.getState(), instance.getReason());
                continue;
            }
            log.info("[{}] 重启恢复: 事务未完成(state={})，提交后台继续推进",
                    sagaId, instance.getState());
            Object lock = lockFor(sagaId);
            driving.add(sagaId);
            recoveryExecutors.submit(() -> {
                try {
                    engine.drive(instance, lock);
                } catch (Exception e) {
                    log.error("[{}] 重启恢复推进异常: {}", sagaId, e.toString(), e);
                } finally {
                    driving.remove(sagaId);
                }
            });
        }
        reaper.start();
    }

    /** 供测试显式启动巡检器。 */
    public void startReaper() {
        reaper.start();
    }

    /** 供测试手动触发一轮巡检。 */
    public void sweepOnce() {
        reaper.sweep();
    }

    private SagaResult toResult(SagaInstance i) {
        return new SagaResult(
                i.getSagaId(),
                i.getDefinition().type(),
                i.getState(),
                i.getReason(),
                i.getReasonDetail(),
                i.getCurrentStepIndex(),
                Map.copyOf(i.getContext().snapshot()));
    }

    public void shutdown() {
        reaper.stop();
        recoveryExecutors.shutdownNow();
        engine.shutdown();
    }

    /**
     * 测试支持：停止巡检与恢复线程，但不中断正在执行的参与者步骤，
     * 用于在单进程内模拟“进程在步骤执行中瞬间崩溃”。
     */
    public void detach() {
        reaper.stop();
        recoveryExecutors.shutdownNow();
    }
}
