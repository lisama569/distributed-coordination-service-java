package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 节点在 Raft 状态机中的角色。
 */
public enum NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
