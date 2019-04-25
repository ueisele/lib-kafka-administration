package de.ux.kafka.clients.admin.controlledshutdown;

import de.ux.kafka.clients.admin.metadata.ErrorDescription;
import de.ux.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ControlledShutdownStatus {

    private KafkaUri source;
    private Integer shutdownBrokerId;
    private ErrorDescription error;
    private Set<TopicPartition> partitionsRemaining;

    public KafkaUri source() {
        return source;
    }

    public ControlledShutdownStatus withSource(KafkaUri source) {
        this.source = source;
        return this;
    }

    public Integer shutdownBrokerId() {
        return shutdownBrokerId;
    }

    public ControlledShutdownStatus withShutdownBrokerId(Integer shutdownBrokerId) {
        this.shutdownBrokerId = shutdownBrokerId;
        return this;
    }

    public ErrorDescription error() {
        return error;
    }

    public ControlledShutdownStatus withError(ErrorDescription error) {
        this.error = error;
        return this;
    }

    public Set<TopicPartition> partitionsRemaining() {
        return partitionsRemaining;
    }

    public ControlledShutdownStatus withPartitionsRemaining(Set<TopicPartition> partitionsRemaining) {
        this.partitionsRemaining = partitionsRemaining;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(source != null) {
            map.put("source", source.toString());
        }
        if(shutdownBrokerId != null) {
            map.put("shutdownBrokerId", shutdownBrokerId);
        }
        if(error != null) {
            map.put("error", error.toMap());
        }
        if(partitionsRemaining != null) {
            map.put("topics", partitionsRemaining.stream().map(TopicPartition::toString).collect(toSet()));
        }
        return map;
    }

    @Override
    public String toString() {
        return "ControlledShutdownStatus{" +
                "source=" + source +
                ", shutdownBrokerId=" + shutdownBrokerId +
                ", error=" + error +
                ", partitionsRemaining=" + partitionsRemaining +
                '}';
    }
}
