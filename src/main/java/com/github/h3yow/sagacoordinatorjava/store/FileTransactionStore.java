package com.github.h3yow.sagacoordinatorjava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 基于本地 JSON 文件的事务存储：
 * 每个事务一个文件，先写临时文件再原子 move，避免崩溃产生半写文件。
 */
public class FileTransactionStore implements TransactionStore {

    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_-]{1,128}");

    private final Path dir;
    private final ObjectMapper objectMapper;

    public FileTransactionStore(Path dir, ObjectMapper objectMapper) {
        this.dir = dir;
        this.objectMapper = objectMapper;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create store dir " + dir, e);
        }
    }

    private Path fileOf(String transactionId) {
        if (transactionId == null || !SAFE_ID.matcher(transactionId).matches()) {
            throw new IllegalArgumentException("illegal transactionId: " + transactionId);
        }
        return dir.resolve("tx-" + transactionId + ".json");
    }

    @Override
    public SagaTransaction createIfAbsent(SagaTransaction transaction) {
        Path target = fileOf(transaction.getTransactionId());
        try {
            // createFile 是原子的 create-if-not-exists：并发提交只有一个能成功落盘
            String json = objectMapper.writeValueAsString(transaction);
            Files.writeString(target, json);
            return transaction;
        } catch (FileAlreadyExistsException e) {
            return find(transaction.getTransactionId())
                    .orElseThrow(() -> new IllegalStateException("transaction file vanished: "
                            + transaction.getTransactionId()));
        } catch (IOException e) {
            throw new UncheckedIOException("createIfAbsent failed: " + target, e);
        }
    }

    @Override
    public void save(SagaTransaction transaction) {
        Path target = fileOf(transaction.getTransactionId());
        Path tmp = dir.resolve(".tmp-" + transaction.getTransactionId() + "-"
                + java.util.UUID.randomUUID() + ".json");
        try {
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(transaction);
            Files.writeString(tmp, json);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("save failed: " + target, e);
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 临时文件清理失败不影响主流程
            }
        }
    }

    @Override
    public Optional<SagaTransaction> find(String transactionId) {
        Path target = fileOf(transactionId);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(Files.readString(target), SagaTransaction.class));
        } catch (IOException e) {
            throw new UncheckedIOException("read failed: " + target, e);
        }
    }

    @Override
    public List<SagaTransaction> findAll() {
        List<SagaTransaction> result = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("tx-")
                            && p.getFileName().toString().endsWith(".json"))
                    .forEach(p -> {
                        try {
                            result.add(objectMapper.readValue(Files.readString(p), SagaTransaction.class));
                        } catch (IOException e) {
                            throw new UncheckedIOException("read failed: " + p, e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("list failed: " + dir, e);
        }
        return result;
    }

    @Override
    public void forEach(java.util.function.Consumer<SagaTransaction> consumer) {
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("tx-")
                            && p.getFileName().toString().endsWith(".json"))
                    .forEach(p -> {
                        try {
                            // 逐条读取、逐条交给调用方处理，不在此处累积列表
                            consumer.accept(objectMapper.readValue(Files.readString(p), SagaTransaction.class));
                        } catch (IOException e) {
                            throw new UncheckedIOException("read failed: " + p, e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("list failed: " + dir, e);
        }
    }
}
