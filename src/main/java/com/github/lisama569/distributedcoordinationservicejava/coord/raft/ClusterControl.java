package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

/**
 * 节点用于访问集群层面能力的最小接口（由 Cluster 实现），
 * 避免 RaftNode 与 Cluster 之间的编译期循环依赖。
 */
public interface ClusterControl {

    int totalNodes();

    void send(RaftMessage message);

    long nowMillis();
}
