package com.github.lisama569.distributedcoordinationservicejava.lock;

import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 带租约与栅栏令牌的分布式锁（进程内模拟）。
 *
 * <ul>
 *   <li>锁仅在当前领导者、且领导者租约有效时可颁发；</li>
 *   <li>栅栏令牌 = (term, 任期内序号)，随任期单调递增，新任期序号从 1 开始但 term 更大，
 *       因此 {@link FenceToken#orderingKey()} 全局严格递增；</li>
 *   <li>竞争失败 {@link CoordinationErrorCode#LOCK_CONTENDED}、
 *       租约过期 {@link CoordinationErrorCode#LEASE_EXPIRED}、
 *       令牌过旧 {@link CoordinationErrorCode#STALE_FENCE_TOKEN} 三类原因可区分。</li>
 * </ul>
 */
public class LockService {

    private static final Logger log = LoggerFactory.getLogger(LockService.class);

    private final SimClock clock;
    private final long leaseTtlMillis;

    /** lockName -> 当前持有者状态（租约过期的记录保留到下次竞争，作为栅栏代次证据） */
    private final Map<String, LockState> locks = new HashMap<>();

    /** term -> 该任期已颁发的最大令牌序号 */
    private final Map<Long, Long> termTokenSeq = new HashMap<>();

    public LockService(SimClock clock, long leaseTtlMillis) {
        this.clock = clock;
        this.leaseTtlMillis = leaseTtlMillis;
    }

    /** 尝试获取锁；失败以 {@link AcquireResult#reason()} 返回可区分原因。 */
    public AcquireResult tryAcquire(RaftNode leader, String lockName, String holderId) {
        long now = clock.nowMillis();
        CoordinationException rejected = requireLeaderWithLease(leader, "tryAcquire", lockName);
        if (rejected != null) {
            log.info("[lock acquire lock={} holder={}] 拒绝: {} (term={}, now={})",
                    lockName, holderId, rejected.getMessage(),
                    leader.getCurrentTerm(), now);
            return AcquireResult.fail(rejected.getErrorCode().name());
        }
        LockState current = locks.get(lockName);
        if (current != null && now <= current.expireAtMillis()) {
            // 仍在别人的有效租约内：竞争失败
            log.info("[lock acquire lock={} holder={}] 竞争失败: 持有者={} 栅栏令牌={} 到期={}",
                    lockName, holderId, current.holderId(), current.token(),
                    current.expireAtMillis());
            return AcquireResult.fail(CoordinationErrorCode.LOCK_CONTENDED.name());
        }
        FenceToken token = mintToken(leader.getCurrentTerm(), holderId, lockName);
        long expireAt = now + leaseTtlMillis;
        locks.put(lockName, new LockState(token, holderId, expireAt));
        log.info("[lock acquire lock={} holder={}] 加锁成功 栅栏令牌={} term={} 到期={} (依据: 无有效持有者)",
                lockName, holderId, token, leader.getCurrentTerm(), expireAt);
        return AcquireResult.success(new LockHandle(token, now, expireAt));
    }

    /** 持有者续租：节点仍是有效租约的领导者、锁仍由此 token 持有时生效。 */
    public AcquireResult renew(RaftNode leader, FenceToken token) {
        long now = clock.nowMillis();
        String lockName = token.lockName();
        CoordinationException rejected = requireLeaderWithLease(leader, "renew", lockName);
        if (rejected != null) {
            return AcquireResult.fail(rejected.getErrorCode().name());
        }
        LockState current = locks.get(lockName);
        if (current == null) {
            return AcquireResult.fail(CoordinationErrorCode.LOCK_CONTENDED.name());
        }
        if (current.token().compareTo(token) > 0) {
            log.info("[lock renew lock={} holder={}] 拒绝: 令牌过旧 当前令牌={} 请求令牌={}",
                    lockName, token.holderId(), current.token(), token);
            return AcquireResult.fail(CoordinationErrorCode.STALE_FENCE_TOKEN.name());
        }
        if (now > current.expireAtMillis()) {
            log.info("[lock renew lock={} holder={}] 拒绝: 租约过期 now={} expireAt={} 令牌={}",
                    lockName, token.holderId(), now, current.expireAtMillis(), token);
            return AcquireResult.fail(CoordinationErrorCode.LEASE_EXPIRED.name());
        }
        long expireAt = now + leaseTtlMillis;
        locks.put(lockName, new LockState(current.token(), current.holderId(), expireAt));
        log.info("[lock renew lock={} holder={}] 续期成功 栅栏令牌={} 新到期={} (依据: 持有者与令牌匹配且在租约内)",
                lockName, token.holderId(), token, expireAt);
        return AcquireResult.success(new LockHandle(current.token(), now, expireAt));
    }

    /** 释放锁：仅当前持有者（令牌匹配且未换代）可释放。 */
    public AcquireResult release(RaftNode leader, FenceToken token) {
        long now = clock.nowMillis();
        String lockName = token.lockName();
        CoordinationException rejected = requireLeaderWithLease(leader, "release", lockName);
        if (rejected != null) {
            return AcquireResult.fail(rejected.getErrorCode().name());
        }
        LockState current = locks.get(lockName);
        if (current == null) {
            return AcquireResult.fail(CoordinationErrorCode.LOCK_CONTENDED.name());
        }
        if (current.token().compareTo(token) > 0) {
            return AcquireResult.fail(CoordinationErrorCode.STALE_FENCE_TOKEN.name());
        }
        if (now > current.expireAtMillis()) {
            return AcquireResult.fail(CoordinationErrorCode.LEASE_EXPIRED.name());
        }
        // 释放后保留栅栏代次：记录过期标记，validateWriteToken 仍能识别旧令牌
        locks.put(lockName, new LockState(current.token(), current.holderId(), Long.MIN_VALUE));
        log.info("[lock release lock={} holder={}] 释放成功 栅栏令牌={}",
                lockName, token.holderId(), token);
        return AcquireResult.success(null);
    }

    /**
     * 写入前的栅栏校验（迟到写的核心防线）：
     * <ol>
     *   <li>锁已被更高令牌持有者拿走 -> STALE_FENCE_TOKEN；</li>
     *   <li>令牌匹配但租约已过期 -> LEASE_EXPIRED；</li>
     *   <li>无任何持有者记录 -> LOCK_CONTENDED。</li>
     * </ol>
     */
    public void validateWriteToken(String lockName, FenceToken token, long nowMillis) {
        LockState current = locks.get(lockName);
        if (current == null) {
            throw new CoordinationException(CoordinationErrorCode.LOCK_CONTENDED,
                    "拒绝迟到写: 锁 " + lockName + " 无持有者记录 token=" + token);
        }
        int cmp = current.token().compareTo(token);
        if (cmp > 0) {
            throw new CoordinationException(CoordinationErrorCode.STALE_FENCE_TOKEN,
                    "拒绝迟到写: 栅栏令牌过旧 lock=" + lockName + " 请求令牌=" + token
                            + " 当前令牌=" + current.token());
        }
        if (cmp < 0) {
            // 请求令牌比锁记录还新：不可能由本服务在该锁上颁发
            throw new CoordinationException(CoordinationErrorCode.STALE_FENCE_TOKEN,
                    "拒绝迟到写: 令牌来源无法识别 lock=" + lockName + " token=" + token);
        }
        if (nowMillis > current.expireAtMillis()) {
            throw new CoordinationException(CoordinationErrorCode.LEASE_EXPIRED,
                    "拒绝迟到写: 锁租约已过期 lock=" + lockName + " token=" + token
                            + " now=" + nowMillis + " expireAt=" + current.expireAtMillis());
        }
        log.info("[lock write lock={} holder={}] 栅栏校验通过 令牌={} term={}",
                lockName, token.holderId(), token, token.term());
    }

    /** 锁的当前栅栏序（无锁时返回 0），便于测试与日志解释。 */
    public long currentFenceOrdering(String lockName) {
        LockState current = locks.get(lockName);
        return current == null ? 0L : current.token().orderingKey();
    }

    public boolean isLocked(String lockName, long nowMillis) {
        LockState current = locks.get(lockName);
        return current != null && nowMillis <= current.expireAtMillis();
    }

    private FenceToken mintToken(long term, String holderId, String lockName) {
        long seq = termTokenSeq.merge(term, 1L, Long::sum);
        return new FenceToken(term, seq, holderId, lockName);
    }

    private CoordinationException requireLeaderWithLease(RaftNode node, String action, String lockName) {
        if (!node.isLeader()) {
            return new CoordinationException(CoordinationErrorCode.NOT_LEADER,
                    action + " 拒绝: 节点 " + node.getNodeId() + " 不是领导者 lock=" + lockName
                            + " term=" + node.getCurrentTerm());
        }
        if (!node.hasValidLease(clock.nowMillis())) {
            return new CoordinationException(CoordinationErrorCode.LEASE_EXPIRED,
                    action + " 拒绝: 领导者租约已过期 node=" + node.getNodeId()
                            + " term=" + node.getCurrentTerm()
                            + " expireAt=" + node.getLeaseExpireAtMillis());
        }
        return null;
    }

    /** 单个锁的持有者状态。expireAtMillis 为 Long.MIN_VALUE 表示已释放但保留栅栏代次。 */
    private record LockState(FenceToken token, String holderId, long expireAtMillis) {
    }
}
