package com.github.lisama569.distributedcoordinationservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 协调模拟服务的可配置项（前缀 coordination.*）。
 */
@ConfigurationProperties(prefix = "coordination")
public class CoordinationProperties {

    /** 集群节点数量（本地模拟）。 */
    private int nodeCount = 3;

    /** 选举超时（逻辑毫秒）。 */
    private long electionTimeoutMillis = 100L;

    /** 心跳间隔（逻辑毫秒）。 */
    private long heartbeatIntervalMillis = 30L;

    /** 领导者租约 TTL（逻辑毫秒）：TTL 内无多数派心跳 ACK 则租约失效。 */
    private long leaseTtlMillis = 60L;

    /**
     * 历史压缩规模阈值：已应用位点相对存档位点每推进多少条日志，就把截至
     * （已应用位点 - 阈值）的历史压缩成一份存档并删除被覆盖的旧日志前缀
     * （存档之外始终保留阈值条最新增量，供窗口内的滞后节点用增量追赶）。
     * 默认 64：默认交付即可在写入持续累积时自动压缩并回收旧日志；
     * {@code <=0} 表示显式关闭压缩。存档只含已提交应用内容，恢复时先装存档再补增量。
     */
    private long compactionThresholdEntries = 64L;

    /** 启动时是否运行演示场景并打印结论日志。 */
    private boolean runDemoOnStartup = false;

    public int getNodeCount() { return nodeCount; }
    public void setNodeCount(int nodeCount) { this.nodeCount = nodeCount; }
    public long getElectionTimeoutMillis() { return electionTimeoutMillis; }
    public void setElectionTimeoutMillis(long v) { this.electionTimeoutMillis = v; }
    public long getHeartbeatIntervalMillis() { return heartbeatIntervalMillis; }
    public void setHeartbeatIntervalMillis(long v) { this.heartbeatIntervalMillis = v; }
    public long getLeaseTtlMillis() { return leaseTtlMillis; }
    public void setLeaseTtlMillis(long v) { this.leaseTtlMillis = v; }
    public long getCompactionThresholdEntries() { return compactionThresholdEntries; }
    public void setCompactionThresholdEntries(long v) { this.compactionThresholdEntries = v; }
    public boolean isRunDemoOnStartup() { return runDemoOnStartup; }
    public void setRunDemoOnStartup(boolean v) { this.runDemoOnStartup = v; }
}
