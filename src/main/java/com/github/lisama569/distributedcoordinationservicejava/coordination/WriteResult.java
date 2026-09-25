package com.github.lisama569.distributedcoordinationservicejava.coordination;

/**
 * 一次幂等写的结论。
 *
 * @param committed       是否提交（每次执行都会返回已存在/新提交的结果）
 * @param dedupHit        是否命中此前已提交的同一幂等键（重试/重复提交时为 true）
 * @param term            提交所在任期
 * @param index           日志下标
 * @param idempotencyKey  幂等键
 * @param value           生效的值（首次提交的值）
 */
public record WriteResult(boolean committed, boolean dedupHit, long term, long index,
                          String idempotencyKey, String value) {
}
