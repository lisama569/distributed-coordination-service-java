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
     * 历史压缩阈值（条数）：节点上仍保留的、已提交并应用的日志达到该条数时，
     * 自动把较早历史整理成状态存档。设为 0 表示关闭自动压缩。
     */
    private long compactionThresholdEntries = 8L;

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
