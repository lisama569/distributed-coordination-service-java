package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;

/**
 * Raft 日志条目。{@code fenceToken} 携带持有者发起写入时的栅栏令牌，
 * 状态机在应用时据此拒绝旧令牌持有者的迟到写。
 */
public class LogEntry {

    private long term;
    private long index;
    private CommandType commandType;
    private String lockKey;
    private String holderId;
    private FenceToken fenceToken;
    private String kvKey;
    private String kvValue;
    private String idempotencyKey;
    private long proposedAtMillis;
    private long lockLeaseMillis;

    public LogEntry() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getTerm() { return term; }
    public void setTerm(long term) { this.term = term; }
    public long getIndex() { return index; }
    public void setIndex(long index) { this.index = index; }
    public CommandType getCommandType() { return commandType; }
    public void setCommandType(CommandType commandType) { this.commandType = commandType; }
    public String getLockKey() { return lockKey; }
    public void setLockKey(String lockKey) { this.lockKey = lockKey; }
    public String getHolderId() { return holderId; }
    public void setHolderId(String holderId) { this.holderId = holderId; }
    public FenceToken getFenceToken() { return fenceToken; }
    public void setFenceToken(FenceToken fenceToken) { this.fenceToken = fenceToken; }
    public String getKvKey() { return kvKey; }
    public void setKvKey(String kvKey) { this.kvKey = kvKey; }
    public String getKvValue() { return kvValue; }
    public void setKvValue(String kvValue) { this.kvValue = kvValue; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public long getProposedAtMillis() { return proposedAtMillis; }
    public void setProposedAtMillis(long proposedAtMillis) { this.proposedAtMillis = proposedAtMillis; }
    public long getLockLeaseMillis() { return lockLeaseMillis; }
    public void setLockLeaseMillis(long lockLeaseMillis) { this.lockLeaseMillis = lockLeaseMillis; }

    public static final class Builder {
        private final LogEntry e = new LogEntry();
        public Builder term(long v) { e.term = v; return this; }
        public Builder index(long v) { e.index = v; return this; }
        public Builder commandType(CommandType v) { e.commandType = v; return this; }
        public Builder lockKey(String v) { e.lockKey = v; return this; }
        public Builder holderId(String v) { e.holderId = v; return this; }
        public Builder fenceToken(FenceToken v) { e.fenceToken = v; return this; }
        public Builder kvKey(String v) { e.kvKey = v; return this; }
        public Builder kvValue(String v) { e.kvValue = v; return this; }
        public Builder idempotencyKey(String v) { e.idempotencyKey = v; return this; }
        public Builder proposedAtMillis(long v) { e.proposedAtMillis = v; return this; }
        public Builder lockLeaseMillis(long v) { e.lockLeaseMillis = v; return this; }
        public LogEntry build() { return e; }
    }

    @Override
    public String toString() {
        return "LogEntry(index=" + index + ",term=" + term + "," + commandType
                + ",idemKey=" + idempotencyKey + ",token=" + fenceToken + ")";
    }
}
