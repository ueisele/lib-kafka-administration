package de.ux.kafka.clients.admin.metadata;

import de.ux.kafka.clients.admin.uri.Uri;

import java.util.*;

import static java.util.stream.Collectors.toList;

public class MetadataDescription {

    private Uri source;
    private ClusterDescription cluster;
    private List<TopicDescription> topics;

    public MetadataDescription() {
        this(null);
    }

    public MetadataDescription(Uri source) {
        this(source ,null, null);
    }

    public MetadataDescription(Uri source, ClusterDescription cluster, List<TopicDescription> topics) {
        this.source = source;
        this.cluster = cluster;
        this.topics = topics;
    }

    public Uri source() {
      return source;
    }

    public MetadataDescription withSource(Uri source) {
        this.source = source;
        return this;
    }

    public ClusterDescription cluster() {
        return cluster;
    }

    public MetadataDescription withCluster(ClusterDescription cluster) {
        this.cluster = cluster;
        return this;
    }

    public List<TopicDescription> topics() {
        return topics;
    }

    public MetadataDescription withTopics(List<TopicDescription> topics) {
        this.topics = topics;
        return this;
    }

    public MetadataDescription addTopic(TopicDescription topic) {
        if(topics == null) {
            topics = new ArrayList<>();
        }
        topics.add(topic);
        return this;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if(source != null) {
            map.put("source", source.toString());
        }
        if(cluster != null) {
            map.put("cluster", cluster.toMap());
        }
        if(topics != null) {
            map.put("topics", topics.stream().map(TopicDescription::toMap).collect(toList()));
        }
        return map;
    }

    @Override
    public String toString() {
        return "MetadataDescription{" +
                "source=" + source +
                ", cluster=" + cluster +
                ", topics=" + topics +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetadataDescription that = (MetadataDescription) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(cluster, that.cluster) &&
                Objects.equals(topics, that.topics);
    }

    @Override
    public int hashCode() {

        return Objects.hash(source, cluster, topics);
    }
}
