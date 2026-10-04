package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.recovery.InMemoryDisk;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 存储层存档（历史压缩）不变量：
 * <ol>
 *   <li>存档位点/任期只能单调推进，回退一律 CHECKPOINT_ROLLBACK；</li>
 *   <li>只允许压缩"已应用"位点，把未应用内容塞进存档必须被拒绝；</li>
 *   <li>只能截断已被存档覆盖的日志前缀，存档之后的增量一条不能误删；</li>
 *   <li>安装存档后位点口径正确，增量可从存档位点之后继续追加。</li>
 * </ol>
 */
class SnapshotStorageTest {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStorageTest.class);

    private Snapshot snap(long index, long term, long currentTerm) {
        return new Snapshot(index, term, currentTerm,
                Map.of("k", "v"), Map.of("k", new Snapshot.IdempotentRecord(term, index, "v")));
    }

    @Test
    void snapshotIndexAndTermNeverRollBack() {
        InMemoryDisk disk = new InMemoryDisk();
        disk.appendEntry(new LogEntry(1L, 1L, "k", "v"));
        disk.saveCheckpoint(new Checkpoint(1L, null, 1L, 1L));
        disk.saveSnapshot(snap(1L, 1L, 1L));

        CoordinationException indexBack = assertThrows(CoordinationException.class,
                () -> disk.saveSnapshot(snap(0L, 1L, 1L)));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, indexBack.getErrorCode());
        log.info("[scenario-存档回退] 存档位点回退被拒 errorCode={} 原因={}",
                indexBack.getErrorCode(), indexBack.getMessage());

        CoordinationException termBack = assertThrows(CoordinationException.class,
                () -> disk.saveSnapshot(new Snapshot(1L, 1L, 0L, Map.of(), Map.of())));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, termBack.getErrorCode());
        log.info("[scenario-存档回退] 存档任期回退被拒 errorCode={} 原因={}",
                termBack.getErrorCode(), termBack.getMessage());
    }

    @Test
    void neverCompactUnappliedEntries() {
        InMemoryDisk disk = new InMemoryDisk();
        disk.appendEntry(new LogEntry(1L, 1L, "a", "1"));
        disk.appendEntry(new LogEntry(2L, 1L, "b", "2"));
        // 只应用到 1，却想把位点 2（尚未确认生效）压进存档
        disk.saveCheckpoint(new Checkpoint(1L, null, 2L, 1L));
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> disk.saveSnapshot(snap(2L, 1L, 1L)));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, ex.getErrorCode());
        log.info("[scenario-未应用不压缩] 压缩未应用位点 2（applied=1）被拒 errorCode={} 原因={}",
                ex.getErrorCode(), ex.getMessage());
    }

    @Test
    void compactionDropsOnlyCoveredPrefix_andAppendsContinueAfterSnapshot() {
        InMemoryDisk disk = new InMemoryDisk();
        for (long i = 1L; i <= 4L; i++) {
            disk.appendEntry(new LogEntry(i, 1L, "k" + i, "v" + i));
        }
        disk.saveCheckpoint(new Checkpoint(1L, null, 4L, 4L));
        disk.saveSnapshot(snap(3L, 1L, 1L));
        disk.compactPrefixThrough(3L);

        assertEquals(4L, disk.lastIndex(), "存档+剩余增量后 lastIndex 口径必须仍是全局位点 4");
        assertEquals(4L, disk.firstRetainedIndex(), "存档覆盖 1..3，保留起点应为 4");
        assertEquals(1L, disk.loadEntries(1L).size(), "只应保留 index=4 一条增量");
        assertNull(disk.loadEntry(3L), "被存档覆盖的旧日志前缀必须删除");
        assertEquals("v4", disk.loadEntry(4L).value());
        // 存档之后的增量可以继续按全局位点追加
        disk.appendEntry(new LogEntry(5L, 1L, "k5", "v5"));
        assertEquals(5L, disk.lastIndex());
        log.info("[scenario-前缀截断] 存档覆盖 1..3 后 lastIndex={} 保留条数={} 可续接点=5",
                disk.lastIndex(), disk.loadEntries(1L).size());

        // 不允许截断存档之后的增量
        CoordinationException overCut = assertThrows(CoordinationException.class,
                () -> disk.compactPrefixThrough(4L));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, overCut.getErrorCode());
        log.info("[scenario-前缀截断] 截断未存档位点 4 被拒 errorCode={}", overCut.getErrorCode());
    }

    @Test
    void installSnapshotAlignsOffsetsAndRejectsOlderSnapshot() {
        InMemoryDisk disk = new InMemoryDisk();
        disk.installSnapshot(snap(5L, 2L, 3L));
        assertEquals(5L, disk.lastIndex());
        assertEquals(2L, disk.lastTerm());
        assertEquals(6L, disk.firstRetainedIndex());
        // 安装存档后，首条增量紧接存档位点 5 追加
        disk.appendEntry(new LogEntry(6L, 2L, "post", "p"));
        assertEquals("p", disk.loadEntry(6L).value());
        log.info("[scenario-安装存档] 空盘安装存档 snapIndex=5 后从 index=6 续接增量成功");

        // 拒绝安装更旧的存档
        CoordinationException older = assertThrows(CoordinationException.class,
                () -> disk.installSnapshot(snap(4L, 2L, 3L)));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, older.getErrorCode());
        assertTrue(older.getMessage().contains("旧"));
        log.info("[scenario-安装存档] 更旧存档(snap=4<5)被拒 errorCode={}", older.getErrorCode());
    }
}
