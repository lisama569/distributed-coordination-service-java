package com.github.lisama569.distributedcoordinationservicejava.coord.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 协调服务可配置项（coord.* 前缀）。
 * 本实现全部在进程内模拟，无外部中间件依赖。
 */
@ConfigurationProperties(prefix = "coord")
public class CoordProperties {

    /** 集群节点数（建议奇数，多数派 = n/2 + 1）。 */
    private int clusterSize = 3;

    /** 领导者心跳间隔（模拟毫秒）。 */
    private long heartbeatIntervalMillis = 50;

    /** 选举超时下限（模拟毫秒）。 */
    private long electionTimeoutMinMillis = 180;

    /** 选举超时上限（模拟毫秒）。 */
    private long electionTimeoutMaxMillis = 300;

    /** 领导者租约时长（模拟毫秒）。 */
    private long leaderLeaseMillis = 400;

    /** 锁租约默认时长（模拟毫秒）。 */
    private long lockLeaseMillis = 250;

    public int getClusterSize() { return clusterSize; }
    public void setClusterSize(int clusterSize) { this.clusterSize = clusterSize; }
    public long getHeartbeatIntervalMillis() { return heartbeatIntervalMillis; }
    public void setHeartbeatIntervalMillis(long v) { this.heartbeatIntervalMillis = v; }
    public long getElectionTimeoutMinMillis() { return electionTimeoutMinMillis; }
    public void setElectionTimeoutMinMillis(long v) { this.electionTimeoutMinMillis = v; }
    public long getElectionTimeoutMaxMillis() { return electionTimeoutMaxMillis; }
    public void setElectionTimeoutMaxMillis(long v) { this.electionTimeoutMaxMillis = v; }
    public long getLeaderLeaseMillis() { return leaderLeaseMillis; }
    public void setLeaderLeaseMillis(long v) { this.leaderLeaseMillis = v; }
    public long getLockLeaseMillis() { return lockLeaseMillis; }
    public void setLockLeaseMillis(long v) { this.lockLeaseMillis = v; }
}
