package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmokeTest {

    @Test
    void electsSingleLeader() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        assertNotNull(leader);
        assertTrue(leader.isLeader());
        assertTrue(leader.hasValidLease(cluster.clock().nowMillis()));
        System.out.println("smoke leader = " + leaderId + " term = " + leader.getCurrentTerm());
    }
}
