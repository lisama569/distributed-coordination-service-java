package com.github.lisama569.distributedcoordinationservicejava.coord.sim;

import com.github.lisama569.distributedcoordinationservicejava.coord.raft.SimTimers;

import java.nio.file.Path;

/** 集群级配置：计时参数与检查点目录。 */
public record ClusterConfig(SimTimers timers, Path checkpointDir) {

    public static ClusterConfig defaults(Path checkpointDir) {
        return new ClusterConfig(SimTimers.defaults(), checkpointDir);
    }
}
