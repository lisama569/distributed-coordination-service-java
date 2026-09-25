package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;

/**
 * 客户端发往领导者的写请求。所有写请求都携带幂等键；
 * 锁操作携带持有者标识，KV 写携带栅栏令牌。
 */
public record ClientRequest(
        CommandType commandType,
        String lockKey,
        String holderId,
        FenceToken fenceToken,
        String kvKey,
        String kvValue,
        String idempotencyKey,
        long lockLeaseMillis) {

    public ClientRequest {
        if (commandType == null) {
            throw new IllegalArgumentException("commandType 不能为空");
        }
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("幂等键不能为空");
        }
    }

    public static ClientRequest lockAcquire(String lockKey, String holderId, String idemKey, long leaseMillis) {
        return new ClientRequest(CommandType.LOCK_ACQUIRE, lockKey, holderId, null,
                null, null, idemKey, leaseMillis);
    }

    public static ClientRequest lockRenew(String lockKey, String holderId, FenceToken token,
                                          String idemKey, long leaseMillis) {
        return new ClientRequest(CommandType.LOCK_RENEW, lockKey, holderId, token,
                null, null, idemKey, leaseMillis);
    }

    public static ClientRequest lockRelease(String lockKey, String holderId, FenceToken token, String idemKey) {
        return new ClientRequest(CommandType.LOCK_RELEASE, lockKey, holderId, token,
                null, null, idemKey, 0);
    }

    public static ClientRequest kvWrite(String lockKey, String kvKey, String kvValue,
                                        FenceToken token, String idemKey) {
        return new ClientRequest(CommandType.KV_WRITE, lockKey, null, token,
                kvKey, kvValue, idemKey, 0);
    }
}
