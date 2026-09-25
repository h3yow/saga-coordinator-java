package com.github.h3yow.sagacoordinatorjava.saga.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.sagacoordinatorjava.saga.model.SagaException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 基于本地目录的追加式事件日志。
 *
 * <p>每个事务对应一个 {@code <storeDir>/<sagaId>.log} 文件，每行一条 JSON
 * 事件。append 时先写内存序号，再以同步方式追加并 flush 到磁盘，崩溃后
 * 最多丢失 OS 页缓存中尚未落盘的最后一行（重放时容忍并截断半截行）。
 */
public class FileEventLog implements EventLog {

    private static final String SUFFIX = ".log";

    private final Path directory;
    private final ObjectMapper mapper = new ObjectMapper();
    private long globalSeq;

    public FileEventLog(String directory) {
        this.directory = Path.of(directory);
        try {
            Files.createDirectories(this.directory);
            this.globalSeq = scanMaxSeq();
        } catch (IOException e) {
            throw new SagaException("无法初始化事件日志目录: " + directory, e);
        }
    }

    @Override
    public synchronized SagaEvent append(SagaEvent event) {
        SagaEvent stored = event.withSeq(++globalSeq);
        Path file = directory.resolve(stored.sagaId() + SUFFIX);
        byte[] line;
        try {
            line = (mapper.writeValueAsString(stored) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SagaException("事件序列化失败: " + stored, e);
        }
        try {
            Files.write(file, line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new SagaException("事件落盘失败: " + file, e);
        }
        return stored;
    }

    @Override
    public synchronized List<SagaEvent> read(String sagaId) {
        Path file = directory.resolve(sagaId + SUFFIX);
        if (!Files.exists(file)) {
            return List.of();
        }
        List<SagaEvent> events = new ArrayList<>();
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SagaException("事件日志读取失败: " + file, e);
        }
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            try {
                events.add(mapper.readValue(line, SagaEvent.class));
            } catch (IOException e) {
                // 崩溃可能留下最后一行半截 JSON：截断并停止读取该行之后的内容
                throw new SagaException("事件日志损坏（疑似崩溃残留的半截行）: " + file, e);
            }
        }
        return events;
    }

    @Override
    public synchronized List<String> sagaIds() {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                    .filter(p -> p.getFileName().toString().endsWith(SUFFIX))
                    .map(p -> {
                        String name = p.getFileName().toString();
                        return name.substring(0, name.length() - SUFFIX.length());
                    })
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long scanMaxSeq() throws IOException {
        long max = 0L;
        try (Stream<Path> stream = Files.list(directory)) {
            for (Path file : (Iterable<Path>) stream::iterator) {
                if (!file.getFileName().toString().endsWith(SUFFIX)) {
                    continue;
                }
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    try {
                        SagaEvent event = mapper.readValue(line, SagaEvent.class);
                        max = Math.max(max, event.seq());
                    } catch (IOException parseError) {
                        // 半截行：直接截断文件尾部，保证后续 append 不会继续污染
                        truncateAtLastValidLine(file);
                        break;
                    }
                }
            }
        }
        return max;
    }

    private void truncateAtLastValidLine(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        List<String> valid = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank()) {
                break;
            }
            try {
                mapper.readValue(line, SagaEvent.class);
                valid.add(line);
            } catch (IOException e) {
                break;
            }
        }
        Files.write(file, (String.join(System.lineSeparator(), valid)
                + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.TRUNCATE_EXISTING);
    }
}
