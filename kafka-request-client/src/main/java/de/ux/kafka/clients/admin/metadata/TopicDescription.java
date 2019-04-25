package de.ux.kafka.clients.admin.metadata;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class TopicDescription {

    private String topic;
    private Boolean internal;
    private List<PartitionDescription> partitions;

    private ErrorDescription error;

    public TopicDescription() {
        this(null);
    }

    public TopicDescription(String topic) {
        this(topic, null, null, null);
    }

    public TopicDescription(String topic, Boolean internal, List<PartitionDescription> partitions, ErrorDescription error) {
        this.topic = topic;
        this.internal = internal;
        this.partitions = partitions;
        this.error = error;
    }

    public String topic() {
        return topic;
    }

    public TopicDescription withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public Boolean isInternal() {
        return internal;
    }

    public TopicDescription withIsInternal(Boolean internal) {
        this.internal = internal;
        return this;
    }

    public List<PartitionDescription> partitions() {
        return partitions;
    }

    public TopicDescription withPartitions(List<PartitionDescription> partitions) {
        this.partitions = partitions;
        return this;
    }

    public TopicDescription addPartition(PartitionDescription partition) {
        if(partitions == null){
            partitions = new ArrayList<>();
        }
        partitions.add(partition);
        return this;
    }

    public boolean hasError() {
        return error != null;
    }

    public ErrorDescription error() {
        return error;
    }

    public TopicDescription withError(ErrorDescription error) {
        this.error = error;
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(topic != null) {
            map.put("topic", topic);
        }
        if(internal != null) {
            map.put("internal", internal);
        }
        if(partitions != null) {
            map.put("partitions", partitions.stream().map(PartitionDescription::toMap).collect(toList()));
        }
        if(error != null) {
            map.put("error", error.toMap());
        }
        return map;
    }

    @Override
    public String toString() {
        return "TopicDescription{" +
                "topic='" + topic + '\'' +
                ", internal=" + internal +
                ", partitions=" + partitions +
                ", error=" + error +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicDescription that = (TopicDescription) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(internal, that.internal) &&
                Objects.equals(partitions, that.partitions) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, internal, partitions, error);
    }
}
