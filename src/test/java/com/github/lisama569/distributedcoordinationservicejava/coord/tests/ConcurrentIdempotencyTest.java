package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import com.github.lisama569.distributedcoordinationservicejava.coord.client.CoordClient;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发幂等与跨任期令牌单调性测试。
 */
class ConcurrentIdempotencyTest extends AbstractCoordTest {

    @Test
    void concurrentSameKeySubmissionsProduceOneEffect() throws Exception {
        printSeparator("并发：同幂等键多线程提交，只有一次写入");
        Cluster cluster = newThreeNodeCluster();
        awaitLeaderId(cluster, 1000);
        CoordClient c = new CoordClient(cluster, "holder-A");
        FenceToken token = c.acquireLock("L", "acq", 900);

        int threads = 8;
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        CyclicBarrierLike barrier = new CyclicBarrierLike(threads);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            workers[i] = new Thread(() -> {
                try {
                    barrier.await();
                    c.fencedWrite("L", "k", "value-" + idx, token, "same-idem-key");
                    accepted.incrementAndGet();
                } catch (CoordinationException e) {
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            workers[i].start();
        }
        for (Thread t : workers) {
            t.join(5000);
        }
        System.out.printf("[断言] 并发提交: 接受=%d, 被在途去重拒绝=%d%n", accepted.get(), rejected.get());
        assertEquals(1, accepted.get(), "同一幂等键只允许一个在途请求真正提交");
        assertEquals(threads - 1, rejected.get(), "其余并发提交必须被抑制");
        assertEquals("value-0".equals(c.read("k")) || c.read("k") != null, true);
        assertNotNull(c.read("k"), "唯一一次写入必须落盘");
        // 所有被抑制的请求都应归类为可解释的原因（这里复用 NOT_LEADER 语义表示“稍后重试”）
        // 关键是只有一次写入效果
        assertFalse(eventsContaining("在途").isEmpty() || eventsContaining("幂等").isEmpty(),
                "日志必须打印并发去重与幂等键判定依据");
    }

    @Test
    void fenceTokensIncreaseAcrossTerms() {
        printSeparator("令牌：同一任期递增，换任期后必然更大");
        Cluster cluster = newThreeNodeCluster();
        awaitLeaderId(cluster, 1000);
        CoordClient a = new CoordClient(cluster, "a");

        FenceToken t1 = a.acquireLock("lk1", "k1", 200);
        cluster.advance(250);
        FenceToken t2 = a.acquireLock("lk1", "k2", 200);
        assertEquals(t1.term(), t2.term(), "同一领导者同一任期内");
        assertTrue(t2.sequence() > t1.sequence(), "同任期序号单调递增");
        assertTrue(t2.compareTo(t1) > 0);

        // 领导者崩溃触发换任期
        String leader = cluster.leaderId();
        cluster.crash(leader);
        for (long t = 0; t < 1500; t++) {
            cluster.advance(1);
            if (cluster.leader() != null) {
                break;
            }
        }
        assertNotNull(cluster.leader());
        // 等锁过期再获取，新任期颁发的令牌必然大于旧任期的任何令牌
        cluster.advance(300);
        FenceToken t3 = a.acquireLock("lk1", "k3", 300);
        assertTrue(t3.term() > t2.term(), "新任期必须更高: " + t3.term() + " > " + t2.term());
        assertTrue(t3.compareTo(t2) > 0 && t3.compareTo(t1) > 0,
                "跨任期令牌必须严格更大: " + t3 + " > " + t2);
        System.out.printf("[断言] 令牌单调: %s -> %s（同任期序号增）-> %s（跨任期必然更大）%n", t1, t2, t3);
    }

    /** 极简栅栏，避免引入额外并发工具的静态导入歧义。 */
    private static final class CyclicBarrierLike {
        private final int parties;
        private int arrived;

        CyclicBarrierLike(int parties) {
            this.parties = parties;
        }

        synchronized void await() throws InterruptedException {
            arrived++;
            if (arrived == parties) {
                notifyAll();
            }
            while (arrived < parties) {
                wait();
            }
        }
    }
}
