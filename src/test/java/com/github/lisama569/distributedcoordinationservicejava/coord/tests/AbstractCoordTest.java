package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.FileCheckpointStore;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.ClusterConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** 测试基类：提供独立临时检查点目录、集群工厂与日志捕获。 */
abstract class AbstractCoordTest {

    protected Path checkpointDir;
    protected ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() throws IOException {
        checkpointDir = Files.createTempDirectory("coord-cp-");
        Logger logger = (Logger) LoggerFactory.getLogger("com.github.lisama569");
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
    }

    @AfterEach
    void tearDown() {
        Logger logger = (Logger) LoggerFactory.getLogger("com.github.lisama569");
        logger.detachAppender(appender);
    }

    protected Cluster newThreeNodeCluster() {
        FileCheckpointStore store = new FileCheckpointStore(checkpointDir);
        return new Cluster(List.of("n1", "n2", "n3"), store,
                ClusterConfig.defaults(checkpointDir));
    }

    /** 找到最新任期且租约有效的领导者节点 ID。 */
    protected String awaitLeaderId(Cluster cluster, long maxMillis) {
        boolean ok = cluster.awaitLeader(maxMillis);
        if (!ok) {
            throw new AssertionError("在 " + maxMillis + " 模拟毫秒内未选出领导者");
        }
        return cluster.leaderId();
    }

    protected List<ILoggingEvent> eventsContaining(String text) {
        return appender.list.stream()
                .filter(e -> e.getFormattedMessage().contains(text))
                .toList();
    }

    protected void printSeparator(String title) {
        System.out.println();
        System.out.println("========== " + title + " ==========");
    }
}
