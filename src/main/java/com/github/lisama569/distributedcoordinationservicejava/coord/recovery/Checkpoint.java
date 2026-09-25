package com.github.lisama569.distributedcoordinationservicejava.coord.recovery;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.LogEntry;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 节点持久化检查点：Raft 易失/持久状态 + 已提交日志 + 幂等结论 + KV 快照。
 * 节点崩溃后从该检查点恢复，保证已提交操作不丢失、不重复应用。
 */
public class Checkpoint {

    public String nodeId;
    public long currentTerm;
    public String votedFor;
    public List<LogEntry> log = new ArrayList<>();
    public long commitIndex;
    public long lastApplied;
    public long lastFenceSequence;
    public Map<String, String> kvSnapshot = new LinkedHashMap<>();
    public Map<String, IdempotentRecord> idempotent = new LinkedHashMap<>();

    /** 幂等键对应的已确定结论。 */
    public static class IdempotentRecord {
        public boolean accepted;
        public String reason;
        public FenceToken fenceToken;
        public String detail;
        public String kvValue;
        public long committedIndex;

        public IdempotentRecord() {
        }

        public IdempotentRecord(boolean accepted, String reason, FenceToken fenceToken,
                                String detail, String kvValue, long committedIndex) {
            this.accepted = accepted;
            this.reason = reason;
            this.fenceToken = fenceToken;
            this.detail = detail;
            this.kvValue = kvValue;
            this.committedIndex = committedIndex;
        }
    }
}
