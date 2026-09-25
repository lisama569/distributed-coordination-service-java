package com.github.lisama569.distributedcoordinationservicejava.coord.statemachine;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 确定性复制状态机：维护锁表（持有者 + 栅栏令牌 + 租约到期时间）与 KV。
 *
 * <p>判定依据：</p>
 * <ul>
 *   <li>锁是否仍由某令牌持有，以日志中记录的“提议时刻”{@code proposedAtMillis}
 *       对照租约到期时间判断。由于提议时刻在复制前就已固化，旧持有者在租约失效后
 *       才复制到各节点的“迟到写”会被一致拒绝，重放结果也不依赖当前墙钟。</li>
 *   <li>栅栏令牌只允许“变新”，任何旧令牌持有者的操作都被拒绝（fencing）。</li>
 *   <li>{@code STALE_FENCE_TOKEN}（令牌过旧）、{@code LOCK_LEASE_EXPIRED}（租约过期）、
 *       {@code LOCK_NOT_HELD}（未持有/竞争失败）三类原因显式区分。</li>
 * </ul>
 */
public class StateMachine {

    private static final Logger log = LoggerFactory.getLogger(StateMachine.class);

    /** 单个锁的运行时状态。 */
    public static final class LockState {
        public String holderId;
        public FenceToken token;
        public long leaseUntilMillis;

        public boolean activeAt(long atMillis) {
            return atMillis < leaseUntilMillis;
        }
    }

    private final Map<String, LockState> locks = new HashMap<>();
    private final Map<String, String> kv = new HashMap<>();

    public Map<String, LockState> locks() {
        return locks;
    }

    public Map<String, String> kv() {
        return kv;
    }

    /**
     * 应用一条已提交日志。该方法必须确定且幂等：同样的日志序列产生同样的结果。
     *
     * @param entry     日志条目（租约判定使用其 proposedAtMillis）
     * @param nowMillis 当前模拟时间，仅用于日志展示
     */
    public Outcome apply(LogEntry entry, long nowMillis) {
        return switch (entry.getCommandType()) {
            case LOCK_ACQUIRE -> applyAcquire(entry);
            case LOCK_RENEW -> applyRenew(entry);
            case LOCK_RELEASE -> applyRelease(entry);
            case KV_WRITE -> applyKvWrite(entry, nowMillis);
        };
    }

    private Outcome applyAcquire(LogEntry e) {
        LockState st = locks.get(e.getLockKey());
        long at = e.getProposedAtMillis();
        if (st != null && st.activeAt(at)) {
            // 竞争失败：锁仍被有效持有
            Outcome o = Outcome.reject(RejectReason.LOCK_NOT_HELD, e.getFenceToken(),
                    "获取锁失败(竞争失败): key=" + e.getLockKey()
                            + ", 当前持有者=" + st.holderId + ", 当前令牌=" + st.token
                            + ", 租约到期=" + st.leaseUntilMillis + ", 提议时刻=" + at
                            + ", 幂等键=" + e.getIdempotencyKey());
            log.info("[状态机] LOCK_ACQUIRE 拒绝: 幂等键={}, 任期={}, 令牌={}, 原因={}, 依据=锁仍被 {} 有效持有至 {}",
                    e.getIdempotencyKey(), e.getTerm(), e.getFenceToken(),
                    RejectReason.LOCK_NOT_HELD, st.holderId, st.leaseUntilMillis);
            return o;
        }
        LockState fresh = new LockState();
        fresh.holderId = e.getHolderId();
        fresh.token = e.getFenceToken();
        fresh.leaseUntilMillis = at + e.getLockLeaseMillis();
        locks.put(e.getLockKey(), fresh);
        log.info("[状态机] LOCK_ACQUIRE 通过: 幂等键={}, 任期={}, 新令牌={}, 持有者={}, 租约到期={}",
                e.getIdempotencyKey(), e.getTerm(), fresh.token, fresh.holderId, fresh.leaseUntilMillis);
        return Outcome.ok(e.getFenceToken(), "锁获取成功: key=" + e.getLockKey());
    }

    private Outcome applyRenew(LogEntry e) {
        LockState st = locks.get(e.getLockKey());
        long at = e.getProposedAtMillis();
        if (st == null) {
            return reject(e, RejectReason.LOCK_NOT_HELD, "锁不存在或已释放", at);
        }
        if (e.getFenceToken() != null && e.getFenceToken().compareTo(st.token) < 0) {
            return reject(e, RejectReason.STALE_FENCE_TOKEN,
                    "令牌过旧: 请求令牌=" + e.getFenceToken() + ", 当前令牌=" + st.token, at);
        }
        if (!st.activeAt(at)) {
            return reject(e, RejectReason.LOCK_LEASE_EXPIRED,
                    "锁租约已过期: 到期=" + st.leaseUntilMillis + ", 提议时刻=" + at, at);
        }
        if (!st.holderId.equals(e.getHolderId())) {
            return reject(e, RejectReason.LOCK_NOT_HELD,
                    "持有者不匹配: 请求者=" + e.getHolderId() + ", 持有者=" + st.holderId, at);
        }
        st.leaseUntilMillis = at + e.getLockLeaseMillis();
        log.info("[状态机] LOCK_RENEW 通过: 幂等键={}, 任期={}, 令牌={}, 新租约到期={}",
                e.getIdempotencyKey(), e.getTerm(), st.token, st.leaseUntilMillis);
        return Outcome.ok(st.token, "锁续期成功: key=" + e.getLockKey());
    }

    private Outcome applyRelease(LogEntry e) {
        LockState st = locks.get(e.getLockKey());
        long at = e.getProposedAtMillis();
        if (st == null) {
            return reject(e, RejectReason.LOCK_NOT_HELD, "锁不存在或已释放", at);
        }
        if (e.getFenceToken() != null && e.getFenceToken().compareTo(st.token) < 0) {
            return reject(e, RejectReason.STALE_FENCE_TOKEN,
                    "令牌过旧: 请求令牌=" + e.getFenceToken() + ", 当前令牌=" + st.token, at);
        }
        if (!st.holderId.equals(e.getHolderId())) {
            return reject(e, RejectReason.LOCK_NOT_HELD,
                    "持有者不匹配: 请求者=" + e.getHolderId() + ", 持有者=" + st.holderId, at);
        }
        locks.remove(e.getLockKey());
        log.info("[状态机] LOCK_RELEASE 通过: 幂等键={}, 任期={}, 令牌={}",
                e.getIdempotencyKey(), e.getTerm(), e.getFenceToken());
        return Outcome.ok(e.getFenceToken(), "锁释放成功: key=" + e.getLockKey());
    }

    private Outcome applyKvWrite(LogEntry e, long nowMillis) {
        LockState st = locks.get(e.getLockKey());
        long at = e.getProposedAtMillis();
        if (st == null) {
            return reject(e, RejectReason.LOCK_NOT_HELD,
                    "写操作未找到有效锁: key=" + e.getLockKey(), at);
        }
        if (e.getFenceToken() == null || e.getFenceToken().compareTo(st.token) < 0) {
            return reject(e, RejectReason.STALE_FENCE_TOKEN,
                    "写令牌过旧: 请求令牌=" + e.getFenceToken() + ", 当前令牌=" + st.token, at);
        }
        if (!st.activeAt(at)) {
            return reject(e, RejectReason.LOCK_LEASE_EXPIRED,
                    "写操作时锁租约已过期(迟到写): 到期=" + st.leaseUntilMillis
                            + ", 提议时刻=" + at + ", 当前时间=" + nowMillis, at);
        }
        kv.put(e.getKvKey(), e.getKvValue());
        log.info("[状态机] KV_WRITE 通过: 幂等键={}, 任期={}, 令牌={}, kvKey={}, kvValue={}, 判定=令牌匹配且租约 {} > {}",
                e.getIdempotencyKey(), e.getTerm(), e.getFenceToken(), e.getKvKey(), e.getKvValue(),
                st.leaseUntilMillis, at);
        return new Outcome(true, null, e.getFenceToken(), "写入成功", e.getKvValue());
    }

    private Outcome reject(LogEntry e, RejectReason reason, String detail, long at) {
        log.info("[状态机] {} 拒绝: 幂等键={}, 任期={}, 令牌={}, 原因={}, 判定依据={}",
                e.getCommandType(), e.getIdempotencyKey(), e.getTerm(), e.getFenceToken(), reason, detail);
        return Outcome.reject(reason, e.getFenceToken(),
                detail + ", key=" + e.getLockKey() + ", 提议时刻=" + at
                        + ", 幂等键=" + e.getIdempotencyKey());
    }

    public String readKv(String key) {
        return kv.get(key);
    }
}
