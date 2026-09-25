package com.github.lisama569.distributedcoordinationservicejava.coord.client;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.ClientRequest;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import com.github.lisama569.distributedcoordinationservicejava.coord.statemachine.Outcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * 锁持有者使用的客户端门面：
 * <ul>
 *   <li>自动发现/刷新领导者，对 {@link RejectReason#NOT_LEADER} 做有限重试；</li>
 *   <li>所有写都带幂等键；进程内 {@link #inFlight} 对“同一幂等键的并发提交”去重，
 *       保证一次用户意图只产生一条复制日志；</li>
 *   <li>状态机/幂等表是最终防线：即使请求换了领导者、跨了任期重试，
 *       已提交结论也会被复用，拒绝同样可识别且结论一致。</li>
 * </ul>
 */
public class CoordClient {

    private static final Logger log = LoggerFactory.getLogger(CoordClient.class);

    private final Cluster cluster;
    private final String holderId;
    private String cachedLeaderId;
    private final Set<String> inFlight = new HashSet<>();

    public CoordClient(Cluster cluster, String holderId) {
        this.cluster = cluster;
        this.holderId = holderId;
    }

    public String holderId() {
        return holderId;
    }

    /** 使用默认 500 模拟毫秒租约获取锁。 */
    public FenceToken acquireLock(String lockKey, String idemKey) {
        return acquireLock(lockKey, idemKey, 500);
    }

    /** 获取锁，成功返回随任期单调递增的新栅栏令牌。 */
    public FenceToken acquireLock(String lockKey, String idemKey, long leaseMillis) {
        ClientRequest req = ClientRequest.lockAcquire(lockKey, holderId, idemKey, leaseMillis);
        Outcome o = submit(req, idemKey);
        if (!o.accepted()) {
            throw new CoordinationException(o.reason(), "获取锁失败: " + o.detail());
        }
        log.info("[客户端] 持有者={} 获取锁成功: key={}, 幂等键={}, 令牌={}",
                holderId, lockKey, idemKey, o.fenceToken());
        return o.fenceToken();
    }

    public FenceToken renewLock(String lockKey, FenceToken token, String idemKey, long leaseMillis) {
        ClientRequest req = ClientRequest.lockRenew(lockKey, holderId, token, idemKey, leaseMillis);
        Outcome o = submit(req, idemKey);
        if (!o.accepted()) {
            throw new CoordinationException(o.reason(), "锁续期失败: " + o.detail());
        }
        return o.fenceToken() != null ? o.fenceToken() : token;
    }

    public void releaseLock(String lockKey, FenceToken token, String idemKey) {
        ClientRequest req = ClientRequest.lockRelease(lockKey, holderId, token, idemKey);
        Outcome o = submit(req, idemKey);
        if (!o.accepted()) {
            throw new CoordinationException(o.reason(), "锁释放失败: " + o.detail());
        }
    }

    /** 携带栅栏令牌的幂等写；令牌过旧/租约过期会抛出携带可区分原因的异常。 */
    public void fencedWrite(String lockKey, String kvKey, String kvValue, FenceToken token, String idemKey) {
        ClientRequest req = ClientRequest.kvWrite(lockKey, kvKey, kvValue, token, idemKey);
        Outcome o = submit(req, idemKey);
        if (!o.accepted()) {
            throw new CoordinationException(o.reason(),
                    "栅栏写失败: 原因=" + o.reason() + ", 判定依据=" + o.detail()
                            + ", 幂等键=" + idemKey + ", 令牌=" + token);
        }
    }

    public String read(String kvKey) {
        RaftNode l = cluster.leader();
        if (l == null) {
            throw new CoordinationException(RejectReason.NO_LEADER, "无领导者，无法读取");
        }
        return l.readKv(kvKey);
    }

    private Outcome submit(ClientRequest req, String idemKey) {
        synchronized (inFlight) {
            if (!inFlight.add(idemKey)) {
                log.info("[客户端] 持有者={} 抑制同幂等键的并发/重复提交: 幂等键={}, 判定=进程内在途去重",
                        holderId, idemKey);
                throw new CoordinationException(RejectReason.NOT_LEADER,
                        "同一幂等键的在途请求尚未完成(并发重复提交被抑制): " + idemKey);
            }
        }
        try {
            CoordinationException last = null;
            for (int attempt = 1; attempt <= 5; attempt++) {
                RaftNode leader = pickLeader();
                try {
                    Outcome o = leader.propose(req);
                    log.info("[客户端] 持有者={} 提交完成: 幂等键={}, 尝试={}, 领导者={}/任期={}, 结论={}",
                            holderId, idemKey, attempt, leader.getNodeId(),
                            leader.getCurrentTerm(), o.accepted() ? "接受" : "拒绝:" + o.reason());
                    return o;
                } catch (CoordinationException e) {
                    last = e;
                    if (e.getReason() != RejectReason.NOT_LEADER) {
                        throw e;
                    }
                    cachedLeaderId = null;
                    log.info("[客户端] NOT_LEADER，刷新领导者并重试: 幂等键={}, 尝试={}", idemKey, attempt);
                    cluster.advance(1);
                }
            }
            throw last;
        } finally {
            synchronized (inFlight) {
                inFlight.remove(idemKey);
            }
        }
    }

    private RaftNode pickLeader() {
        RaftNode l = cluster.leader();
        if (l == null) {
            throw new CoordinationException(RejectReason.NO_LEADER,
                    "当前无有效租约领导者: 持有者=" + holderId);
        }
        cachedLeaderId = l.getNodeId();
        return l;
    }
}
