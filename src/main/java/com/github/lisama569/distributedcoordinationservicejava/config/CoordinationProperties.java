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
     * 历史压缩规模阈值：已应用位点相对存档位点每推进多少条日志，就把截至已应用位点的
     * 历史压缩成一份存档并删除被覆盖的旧日志前缀。默认 {@code 100}：默认交付即开启，
     * 写入持续累积时日志规模被稳定压在阈值附近，较早的已落定历史整理成紧凑存档、
     * 旧日志前缀被真正回收。{@code <=0} 表示显式关闭压缩（仅用于对比/特殊场景）。
     * 存档只含已提交应用内容，恢复时先装存档再补增量。
     */
    private long compactionThresholdEntries = 100L;

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
