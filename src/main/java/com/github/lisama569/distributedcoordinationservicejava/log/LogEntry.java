package com.github.lisama569.distributedcoordinationservicejava.log;

/**
 * Raft 风格日志条目。
 *
 * @param index      日志下标，从 1 开始，单调递增
 * @param term       条目写入时的领导者任期
 * @param idempotencyKey 幂等键，可为 null（非幂等写）
 * @param value      写入载荷
 */
public record LogEntry(long index, long term, String idempotencyKey, String value) {
}
