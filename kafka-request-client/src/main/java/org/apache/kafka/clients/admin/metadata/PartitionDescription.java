package org.apache.kafka.clients.admin.metadata;

import java.util.*;

public class PartitionDescription {

    private Integer partition;
    private Integer leader;
    private List<Integer> replicas;
    private List<Integer> isr;
    private List<Integer> offlineReplicas;

    private ErrorDescription error;

    public PartitionDescription() {
        this(null);
    }

    public PartitionDescription(Integer partition) {
        this(partition, null, null, null, null, null);
    }

    public PartitionDescription(Integer partition, Integer leader, List<Integer> replicas, List<Integer> isr, List<Integer> offlineReplicas, ErrorDescription error) {
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
        this.offlineReplicas = offlineReplicas;
        this.error = error;
    }

    public Integer partition() {
        return partition;
    }

    public PartitionDescription withPartition(Integer partition) {
        this.partition = partition;
        return this;
    }

    public Integer leader() {
        return leader;
    }

    public PartitionDescription withLeader(Integer leader) {
        this.leader = leader;
        return this;
    }

    public List<Integer> replicas() {
        return replicas;
    }

    public PartitionDescription withReplicas(List<Integer> replicas) {
        this.replicas = replicas;
        return this;
    }

    public PartitionDescription addReplica(int nodeId) {
        if(replicas == null) {
            replicas = new ArrayList<>();
        }
        replicas.add(nodeId);
        return this;
    }

    public List<Integer> isr() {
        return isr;
    }

    public PartitionDescription withIsr(List<Integer> isr) {
        this.isr = isr;
        return this;
    }

    public PartitionDescription addIsr(int nodeId) {
        if(isr == null) {
            isr = new ArrayList<>();
        }
        isr.add(nodeId);
        return this;
    }

    public List<Integer> offlineReplicas() {
        return offlineReplicas;
    }

    public PartitionDescription withOfflineReplicas(List<Integer> offlineReplicas) {
        this.offlineReplicas = offlineReplicas;
        return this;
    }

    public PartitionDescription addOfflineReplica(int nodeId) {
        if(offlineReplicas == null) {
            offlineReplicas = new ArrayList<>();
        }
        offlineReplicas.add(nodeId);
        return this;
    }

    public boolean hasError() {
        return error != null;
    }

    public ErrorDescription error() {
        return error;
    }

    public PartitionDescription withError(ErrorDescription error) {
        this.error = error;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(partition != null) {
            map.put("partition", partition);
        }
        if(leader != null) {
            map.put("leader", leader);
        }
        if(replicas != null) {
            map.put("replicas", replicas);
        }
        if(isr != null) {
            map.put("isr", isr);
        }
        if(offlineReplicas != null) {
            map.put("offlineReplicas", offlineReplicas);
        }
        if(error != null) {
            map.put("error", error.toMap());
        }
        return map;
    }

    @Override
    public String toString() {
        return "PartitionDescription{" +
                "partition=" + partition +
                ", leader=" + leader +
                ", replicas=" + replicas +
                ", isr=" + isr +
                ", offlineReplicas=" + offlineReplicas +
                ", error=" + error +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionDescription that = (PartitionDescription) o;
        return Objects.equals(partition, that.partition) &&
                Objects.equals(leader, that.leader) &&
                Objects.equals(replicas, that.replicas) &&
                Objects.equals(isr, that.isr) &&
                Objects.equals(offlineReplicas, that.offlineReplicas) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {

        return Objects.hash(partition, leader, replicas, isr, offlineReplicas, error);
    }
}
