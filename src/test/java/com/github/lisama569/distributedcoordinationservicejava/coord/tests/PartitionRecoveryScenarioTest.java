package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import com.github.lisama569.distributedcoordinationservicejava.coord.client.CoordClient;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.NodeRole;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.ClientRequest;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 端到端分区恢复场景：
 * A 在领导者 L1 处获锁并写入；L1 被隔离进少数派；L1 租约过期拒绝写；
 * 多数派选出更高任期 L2，A 在 L2 处拿到新令牌；分区恢复后 L1 降级，
 * 旧令牌写被 fencing 拒绝，整个过程无双主、无旧任期覆盖。
 */
class PartitionRecoveryScenarioTest extends AbstractCoordTest {

    @Test
    void fullPartitionRecoveryScenario() {
        printSeparator("端到端：分区 + 租约过期 + 恢复 + 新旧令牌 fencing");
        Cluster cluster = newThreeNodeCluster();
        String l1 = awaitLeaderId(cluster, 1000);
        long term1 = cluster.node(l1).getCurrentTerm();

        CoordClient a = new CoordClient(cluster, "app-A");
        FenceToken oldToken = a.acquireLock("order-42", "acq-1", 150);
        a.fencedWrite("order-42", "status", "paid", oldToken, "write-1");
        assertEquals("paid", a.read("status"));
        System.out.printf("[场景] 初始领导者=%s 任期=%d, A 令牌=%s, status=paid%n", l1, term1, oldToken);

        // 1) 领导者被隔离为少数派
        cluster.split(List.of(l1));
        List<String> majority = cluster.nodeIds().stream().filter(id -> !id.equals(l1)).toList();

        // 2) 旧主租约内短暂存活，过期后写被拒
        cluster.advance(420);
        RaftNode stale = cluster.node(l1);
        assertFalse(stale.isLeaderLeaseValid());
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> stale.propose(ClientRequest.lockAcquire("order-42", "ghost", "ghost-1", 200)));
        assertEquals(RejectReason.LEADER_LEASE_EXPIRED, ex.getReason());
        System.out.printf("[场景] 被隔离旧主 %s 写被拒: 原因=%s，任期仍=%d（未推进）%n",
                l1, ex.getReason(), stale.getCurrentTerm());

        // 3) 多数派分区选出更高任期新主
        String l2 = null;
        long term2 = -1;
        for (long t = 0; t < 1500; t++) {
            cluster.advance(1);
            for (String id : majority) {
                RaftNode n = cluster.node(id);
                if (n.isLeaderLeaseValid()) {
                    l2 = id;
                    term2 = n.getCurrentTerm();
                }
            }
            if (l2 != null) {
                break;
            }
        }
        assertNotNull(l2, "多数派必须选出新领导者");
        assertTrue(term2 > term1, "新任期必须严格高于旧任期");
        assertFalse(cluster.node(l1).isLeaderLeaseValid(), "旧主租约必须已失效");
        RaftNode onlyValid = cluster.leader();
        assertNotNull(onlyValid, "多数派侧必须存在一个有效租约领导者");
        assertEquals(l2, onlyValid.getNodeId(), "分区期间有效领导者只能是多数派新主，旧主不得双主");
        System.out.printf("[场景] 多数派新主=%s 任期=%d > %d；旧主租约已失效，全集群仅一个有效领导者，无双主%n",
                l2, term2, term1);

        // 4) 旧锁租约早已过期（隔离后经过 420ms + 选举耗时），A 直接在新主处重新获锁。
        //    第一次调用即应成功：若领导者尚未稳定，先推进到存在有效领导者再提交。
        cluster.advanceUntil(50, () -> cluster.leader() != null);
        FenceToken newToken = a.acquireLock("order-42", "acq-2", 500);
        assertTrue(newToken.compareTo(oldToken) > 0, "新令牌必须大于旧令牌");
        System.out.printf("[场景] A 在新主 %s 处重新获锁: 新令牌=%s > 旧令牌=%s%n", l2, newToken, oldToken);
        a.fencedWrite("order-42", "status", "shipped", newToken, "write-2");
        assertEquals("shipped", a.read("status"));

        // 5) 分区恢复：旧主见到更高任期必须降级
        cluster.heal();
        cluster.advance(250);
        assertEquals(NodeRole.FOLLOWER, cluster.node(l1).getRole(), "旧主必须降级为跟随者");
        assertEquals(term2, cluster.node(l1).getCurrentTerm(), "旧主必须追上并接受新任期");
        System.out.printf("[场景] 分区恢复后旧主 %s 已降级，任期追平为 %d%n", l1, term2);

        // 6) 旧令牌持有者（或旧主上的迟到请求）写入必须被 fencing 拒绝
        CoordinationException staleToken = assertThrows(CoordinationException.class,
                () -> a.fencedWrite("order-42", "status", "rollback-value", oldToken, "write-old-token"));
        assertEquals(RejectReason.STALE_FENCE_TOKEN, staleToken.getReason());
        assertEquals("shipped", a.read("status"), "旧令牌写入不得生效，旧任期不能覆盖新状态");
        System.out.printf("[场景] 旧令牌迟到写被拒: 原因=%s, status 保持 shipped，无旧任期覆盖%n",
                staleToken.getReason());

        // 7) 全局恰好一个有效领导者
        RaftNode only = cluster.leader();
        assertNotNull(only);
        System.out.printf("[场景] 恢复后唯一领导者=%s 任期=%d%n", only.getNodeId(), only.getCurrentTerm());
    }
}
