package com.github.lisama569.distributedcoordinationservicejava.coord.sim;

import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 进程内模拟网络：按“分区”维护节点成员关系，同区可达、跨区消息丢弃；
 * 崩溃节点不接收消息。模拟为可靠但可分区的链路：链路存在时消息即时送达。
 */
public class Network {

    private static final Logger log = LoggerFactory.getLogger(Network.class);

    /** 网络组件：节点实现该接口接收消息。 */
    public interface Endpoint {
        void receive(RaftMessage message);
    }

    private final Map<String, Endpoint> endpoints = new HashMap<>();
    private final Map<String, String> partitionOf = new HashMap<>();
    private final Set<String> crashed = new HashSet<>();

    public void register(String nodeId, Endpoint endpoint) {
        endpoints.put(nodeId, endpoint);
        partitionOf.putIfAbsent(nodeId, null);
        crashed.remove(nodeId);
    }

    public void crash(String nodeId) {
        crashed.add(nodeId);
        log.info("[网络] 节点崩溃，收发被切断: {}", nodeId);
    }

    public void recover(String nodeId) {
        crashed.remove(nodeId);
        log.info("[网络] 节点恢复: {}", nodeId);
    }

    public boolean isCrashed(String nodeId) {
        return crashed.contains(nodeId);
    }

    /** 将指定节点放入命名分区；处于不同分区（含 null/默认区）的节点之间不可达。 */
    public void isolate(String partitionName, Collection<String> nodeIds) {
        for (String id : nodeIds) {
            partitionOf.put(id, partitionName);
        }
        log.info("[网络] 建立分区: {} 成员={}", partitionName, nodeIds);
    }

    /** 取消全部分区，恢复全互通。 */
    public void healAll() {
        partitionOf.replaceAll((k, v) -> null);
        log.info("[网络] 分区全部恢复");
    }

    public boolean isReachable(String from, String to) {
        if (from.equals(to)) {
            return true;
        }
        if (crashed.contains(from) || crashed.contains(to)) {
            return false;
        }
        String pf = partitionOf.get(from);
        String pt = partitionOf.get(to);
        // null 表示默认区（未分区），默认区内及未显式隔离的节点之间全互通
        if (pf == null && pt == null) {
            return true;
        }
        if (pf == null || pt == null) {
            return false;
        }
        return pf.equals(pt);
    }

    public void send(RaftMessage message) {
        Endpoint target = endpoints.get(message.to());
        if (target == null) {
            log.debug("[网络] 目标不存在，丢弃: {} -> {}", message.from(), message.to());
            return;
        }
        if (!isReachable(message.from(), message.to())) {
            log.debug("[网络] 分区/崩溃导致丢弃: {} -> {} ({} 任期={})",
                    message.from(), message.to(),
                    message.getClass().getSimpleName(), message.term());
            return;
        }
        target.receive(message);
    }
}
