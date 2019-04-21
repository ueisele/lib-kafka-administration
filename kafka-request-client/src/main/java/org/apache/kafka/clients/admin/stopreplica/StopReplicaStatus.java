package org.apache.kafka.clients.admin.stopreplica;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.metadata.ErrorDescription;
import org.apache.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class StopReplicaStatus {

    private KafkaUri source;
    private ErrorDescription error;
    private Map<TopicPartition, ErrorDescription> partitions;

    public KafkaUri source() {
        return source;
    }

    public StopReplicaStatus withSource(KafkaUri source) {
        this.source = source;
        return this;
    }

    public ErrorDescription error() {
        return error;
    }

    public StopReplicaStatus withError(ErrorDescription error) {
        this.error = error;
        return this;
    }

    public Map<TopicPartition, ErrorDescription> partitions() {
        return partitions;
    }

    public StopReplicaStatus withPartitions(Map<TopicPartition, ErrorDescription> partitions) {
        this.partitions = partitions;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(source != null) {
            map.put("source", source.toString());
        }
        if(error != null) {
            map.put("error", error.toMap());
        }
        if(partitions != null) {
            map.put("topicPartitions", partitions.entrySet().stream()
                    .map(entry -> ImmutablePair.of(entry.getKey().toString(), entry.getValue().toMap()))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        }
        return map;
    }

    @Override
    public String toString() {
        return "ControlledShutdownStatus{" +
                "source=" + source +
                ", error=" + error +
                ", topicPartitions=" + partitions +
                '}';
    }
}
