package com.github.lisama569.distributedcoordinationservicejava.demo;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.config.CoordinationProperties;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.lock.AcquireResult;
import com.github.lisama569.distributedcoordinationservicejava.lock.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.lock.LockHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 本地演示：以 {@code --coordination.run-demo-on-startup=true} 启动时，
 * 在进程内依次跑选举、租约锁、分区恢复、幂等+崩溃恢复四个场景并打印结论与判定依据。
 * 全程使用逻辑时钟，无真实网络、无线程、无外部中间件。
 */
@Component
@ConditionalOnProperty(prefix = "coordination", name = "run-demo-on-startup", havingValue = "true")
public class DemoScenarioRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoScenarioRunner.class);

    private final CoordinationProperties properties;

    public DemoScenarioRunner(CoordinationProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("================ 分布式协调服务本地演示开始（节点数={}）================",
                properties.getNodeCount());
        demoElectionAndQuorum();
        demoLeaseLockFailures();
        demoPartitionHealNoDualLeader();
        demoIdempotencyAndCrashRecovery();
        demoCompactionAndCatchUp();
        log.info("================ 本地演示结束 ================");
    }

    private void demoElectionAndQuorum() {
        log.info("---- 场景1：多数派选举 + 失多数派拒绝写 ----");
        ClusterSimulator cluster = newCluster();
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        log.info("[demo-选举] 唯一领导者={} term={}（依据：多数派投票，同 term 一票）",
                leaderId, leader.getCurrentTerm());
        cluster.isolate(leaderId);
        cluster.advance(properties.getLeaseTtlMillis() + properties.getHeartbeatIntervalMillis() + 1);
        try {
            leader.propose("demo-key", "demo-value");
            log.warn("[demo-选举] 意外：隔离后的领导者仍接受了写入");
        } catch (CoordinationException e) {
            log.info("[demo-选举] 隔离后写入被正确拒绝 errorCode={} 原因={}",
                    e.getErrorCode(), e.getMessage());
        }
        String newLeader = cluster.awaitLeader();
        log.info("[demo-选举] 多数派新领导者={} term={}（旧 term={} 不允许覆盖）",
                newLeader, cluster.node(newLeader).getCurrentTerm(),
                cluster.node(leaderId).getCurrentTerm());
        cluster.healPartition();
        cluster.advance(properties.getHeartbeatIntervalMillis() + 1);
        long leaders = cluster.nodes().stream().filter(RaftNode::isLeader).count();
        log.info("[demo-选举] 分区恢复后领导者数={}（必须为 1）", leaders);
    }

    private void demoLeaseLockFailures() {
        log.info("---- 场景2：竞争失败 / 租约过期 / 令牌过旧 三类可区分错误 ----");
        ClusterSimulator cluster = newCluster();
        RaftNode leader = cluster.node(cluster.awaitLeader());
        LockHandle a = cluster.lockService().tryAcquire(leader, "demo-lock", "A").handle();
        log.info("[demo-锁] A 获锁 栅栏令牌={}", a.token());
        AcquireResult b = cluster.lockService().tryAcquire(leader, "demo-lock", "B");
        log.info("[demo-锁] B 竞争结果 acquired={} reason={}（竞争失败可区分）", b.acquired(), b.reason());

        cluster.advance(properties.getLeaseTtlMillis() + 1);
        try {
            cluster.lockService().validateWriteToken("demo-lock", a.token(),
                    cluster.clock().nowMillis());
        } catch (CoordinationException e) {
            log.info("[demo-锁] A 租约过期后迟到写被拒 errorCode={}", e.getErrorCode());
        }
    }

    private void demoPartitionHealNoDualLeader() {
        log.info("---- 场景3：分区恢复后无旧任期覆盖新任期 ----");
        ClusterSimulator cluster = newCluster();
        RaftNode oldLeader = cluster.node(cluster.awaitLeader());
        long oldTerm = oldLeader.getCurrentTerm();
        cluster.isolate(oldLeader.getNodeId());
        cluster.advance(properties.getLeaseTtlMillis() + properties.getElectionTimeoutMillis() + 5);
        RaftNode newLeader = cluster.node(cluster.awaitLeader());
        log.info("[demo-恢复] 新领导者 term={} > 旧 term={}", newLeader.getCurrentTerm(), oldTerm);
        cluster.healPartition();
        cluster.advance(properties.getHeartbeatIntervalMillis() + 1);
        log.info("[demo-恢复] 旧领导者 role={} term={}，全集群仅一个有效领导者: {}",
                oldLeader.getRole(), oldLeader.getCurrentTerm(),
                cluster.leader() != null ? cluster.leader().getNodeId() : "无");
    }

    private void demoIdempotencyAndCrashRecovery() {
        log.info("---- 场景4：幂等重试只写一次 + 崩溃恢复不重复应用 ----");
        ClusterSimulator cluster = newCluster();
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        String key = "demo-idempotent-key";
        WriteResult first = cluster.coordinator(leaderId).submit(leader, key, "v1");
        WriteResult retry = cluster.coordinator(leaderId).submit(leader, key, "v2");
        log.info("[demo-幂等] 幂等键={} 首次 index={}/term={}，重试 dedupHit={}（值仍为 {}）",
                key, first.index(), first.term(), retry.dedupHit(), retry.value());

        cluster.crash(leaderId);
        cluster.restart(leaderId);
        RaftNode restarted = cluster.node(leaderId);
        WriteResult afterRestart = cluster.coordinator(leaderId).submit(restarted, key, "v3");
        log.info("[demo-恢复] 重启后 term={} appliedIndex={} 重试 dedupHit={} index={}（未重复应用）",
                restarted.getCurrentTerm(), restarted.getLastApplied(),
                afterRestart.dedupHit(), afterRestart.index());
    }

    private void demoCompactionAndCatchUp() {
        log.info("---- 场景5：历史压缩 + 落后节点存档/增量追赶 + 超窗挡回 ----");
        // 演示用较小阈值 5 让压缩更快可见；默认交付阈值见
        // ClusterSimulator.DEFAULT_COMPACTION_THRESHOLD_ENTRIES（64，默认即开启压缩）
        ClusterSimulator cluster = new ClusterSimulator(3, 5L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        for (int i = 1; i <= 18; i++) {
            cluster.coordinator(leaderId).submit(leader, "hist-" + i, "h" + i);
        }
        log.info("[demo-压缩] 18 条写入后领导者存档位点 snapIndex={}（阈值=5，存档外保留 5 条增量）",
                leader.getSnapshotIndex());

        // 新节点（空盘）加入：先装存档再补增量，追平后旧幂等键重试仍只生效一次
        String joiner = "node4";
        cluster.addNode(joiner);
        for (int t = 0; t < 10; t++) {
            cluster.advance(properties.getHeartbeatIntervalMillis());
        }
        RaftNode caught = cluster.node(joiner);
        WriteResult replay = cluster.coordinator(joiner)
                .submit(caught, "hist-1", "should-replay");
        log.info("[demo-追赶] 新节点 {} 追平 appliedIndex={}/{}(领导者)，旧幂等键重试 dedupHit={} 值={}",
                joiner, caught.getLastApplied(), leader.getLastApplied(),
                replay.dedupHit(), replay.value());
    }

    private ClusterSimulator newCluster() {
        ClusterSimulator cluster = new ClusterSimulator(properties.getNodeCount(),
                properties.getCompactionThresholdEntries());
        cluster.setElectionTimeoutMillis(properties.getElectionTimeoutMillis());
        cluster.setHeartbeatIntervalMillis(properties.getHeartbeatIntervalMillis());
        cluster.setLeaseTtlMillis(properties.getLeaseTtlMillis());
        return cluster;
    }
}
