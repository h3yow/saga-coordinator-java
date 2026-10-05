package com.github.h3yow.sagacoordinatorjava.store;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.model.FailureReason;
import com.github.h3yow.sagacoordinatorjava.model.SagaTransaction;
import com.github.h3yow.sagacoordinatorjava.model.TransactionStatus;
import com.github.h3yow.sagacoordinatorjava.model.TransactionSummary;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
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
    public void scanSummaries(Consumer<TransactionSummary> consumer) {
        // 流式逐个文件、逐字段读取：任意时刻只持有一条摘要，
        // 不解析 steps / events 数组，上千条事务也不会把内存顶高。
        try (Stream<Path> stream = Files.list(dir)) {
            List<Path> files = stream
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.startsWith("tx-") && name.endsWith(".json");
                    })
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
            for (Path file : files) {
                try {
                    consumer.accept(readSummary(file));
                } catch (IOException e) {
                    throw new UncheckedIOException("read summary failed: " + file, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("scan failed: " + dir, e);
        }
    }

    /**
     * 只解析事务 JSON 顶层的摘要字段；遇到 steps / events 数组直接整体跳过，
     * 避免把可能很长的事件轨迹读入内存。
     */
    private TransactionSummary readSummary(Path file) throws IOException {
        TransactionSummary summary = new TransactionSummary();
        try (JsonParser parser = objectMapper.getFactory().createParser(file.toFile())) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("unexpected json root in " + file);
            }
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                String field = parser.currentName();
                JsonToken token = parser.nextToken();
                if (token == JsonToken.START_ARRAY || token == JsonToken.START_OBJECT) {
                    parser.skipChildren();
                    continue;
                }
                switch (field) {
                    case "transactionId" -> summary.setTransactionId(parser.getValueAsString());
                    case "sagaType" -> summary.setSagaType(parser.getValueAsString());
                    case "status" -> summary.setStatus(TransactionStatus.valueOf(parser.getValueAsString()));
                    case "failureReason" -> {
                        String reason = parser.getValueAsString();
                        if (reason != null) {
                            summary.setFailureReason(FailureReason.valueOf(reason));
                        }
                    }
                    case "failureDetail" -> summary.setFailureDetail(parser.getValueAsString());
                    case "createdAt" -> summary.setCreatedAt(parser.getLongValue());
                    case "updatedAt" -> summary.setUpdatedAt(parser.getLongValue());
                    default -> {
                        // 其它标量字段（deadlineAt / attempts ...）与摘要无关，忽略
                    }
                }
            }
        }
        return summary;
    }
}
