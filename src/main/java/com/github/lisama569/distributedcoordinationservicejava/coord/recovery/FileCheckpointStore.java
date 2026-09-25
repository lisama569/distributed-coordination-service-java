package com.github.lisama569.distributedcoordinationservicejava.coord.recovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于本地 JSON 文件的检查点存储：写入采用“临时文件 + 原子 move”，
 * 崩溃只会留下上一份完整检查点，不会出现半写文件。
 *
 * <p>额外提供 {@link #ensureNoRollback} 安全门：尝试用任期更旧、
 * 日志更短或提交位点更小的检查点恢复时，显式抛出 {@link RejectReason#ROLLBACK_REFUSED}。</p>
 */
public class FileCheckpointStore implements CheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(FileCheckpointStore.class);

    private final Path directory;
    private final ObjectMapper mapper;

    public FileCheckpointStore(Path directory) {
        this.directory = directory;
        this.mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建检查点目录: " + directory, e);
        }
    }

    private Path fileOf(String nodeId) {
        return directory.resolve(nodeId + ".checkpoint.json");
    }

    @Override
    public synchronized void save(Checkpoint checkpoint) {
        Path target = fileOf(checkpoint.nodeId);
        Path tmp = directory.resolve(checkpoint.nodeId + ".checkpoint.json.tmp");
        try {
            mapper.writeValue(tmp.toFile(), checkpoint);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("检查点写入失败: " + target, e);
        }
    }

    @Override
    public synchronized Checkpoint load(String nodeId) {
        Path target = fileOf(nodeId);
        if (!Files.exists(target)) {
            return null;
        }
        try {
            return mapper.readValue(target.toFile(), Checkpoint.class);
        } catch (IOException e) {
            throw new UncheckedIOException("检查点读取失败(可能已损坏): " + target, e);
        }
    }

    @Override
    public synchronized void ensureNoRollback(String nodeId, long term, long lastLogIndex, long commitIndex) {
        Checkpoint persisted = load(nodeId);
        if (persisted == null) {
            return;
        }
        if (term < persisted.currentTerm) {
            rollback(nodeId, "任期", persisted.currentTerm, term);
        }
        long persistedLastLog = persisted.log.isEmpty() ? 0L : persisted.log.get(persisted.log.size() - 1).getIndex();
        if (lastLogIndex < persistedLastLog) {
            rollback(nodeId, "日志位点", persistedLastLog, lastLogIndex);
        }
        if (commitIndex < persisted.commitIndex) {
            rollback(nodeId, "提交位点", persisted.commitIndex, commitIndex);
        }
        log.info("[检查点] 回退检查通过: 节点={}, 任期={}, 日志末位={}, 提交位点={}",
                nodeId, term, lastLogIndex, commitIndex);
    }

    private void rollback(String nodeId, String what, long persisted, long attempted) {
        String msg = "拒绝任期/位点回退: 节点=" + nodeId + ", " + what
                + " 已持久化=" + persisted + ", 试图恢复为=" + attempted;
        log.warn("[检查点] {}", msg);
        throw new CoordinationException(RejectReason.ROLLBACK_REFUSED, msg);
    }

    @Override
    public synchronized void clear(String nodeId) {
        try {
            Files.deleteIfExists(fileOf(nodeId));
        } catch (IOException e) {
            throw new UncheckedIOException("清理检查点失败: " + nodeId, e);
        }
    }
}
