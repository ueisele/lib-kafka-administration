package org.apache.kafka.clients.admin.metadata;

import org.apache.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.clients.admin.uri.Uri;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;

public class MetadataResponseToMetadataDescriptionMapper implements BiFunction<Uri, MetadataResponse, MetadataDescription> {

    @Override
    public MetadataDescription apply(Uri source, MetadataResponse metadataResponse) {
        return toMetadataDescription(source, metadataResponse);
    }

    public MetadataDescription toMetadataDescription(Uri source, MetadataResponse metadataResponse) {
        return new MetadataDescription()
                .withSource(source)
                .withCluster(toClusterDescription(metadataResponse))
                .withTopics(toTopicDescriptions(metadataResponse.topicMetadata()));
    }

    private List<KafkaUri> toKafkaUris(Collection<Node> nodes) {
        return nodes.stream()
                .map(this::toKafkaUri)
                .sorted(Comparator.comparing(KafkaUri::toString))
                .collect(toList());
    }

    private KafkaUri toKafkaUri(Node node) {
        return new KafkaUri(node.host(), node.port(), node.id(), node.rack());
    }

    private ClusterDescription toClusterDescription(MetadataResponse metadataResponse) {
        return new ClusterDescription()
                .withClusterId(metadataResponse.clusterId())
                .withController(toKafkaUri(metadataResponse.controller()))
                .withNodes(toKafkaUris(metadataResponse.brokers()));
    }

    private List<TopicDescription> toTopicDescriptions(Collection<MetadataResponse.TopicMetadata> topicMetadatas) {
        return topicMetadatas.stream()
                .map(this::toTopicDescription)
                .sorted(Comparator.comparing(TopicDescription::topic))
                .collect(toList());
    }

    private TopicDescription toTopicDescription(MetadataResponse.TopicMetadata topicMetadata) {
        return new TopicDescription()
                .withTopic(topicMetadata.topic())
                .withIsInternal(topicMetadata.isInternal())
                .withPartitions(toPartitionDescriptions(topicMetadata.partitionMetadata()))
                .withError(!topicMetadata.error().equals(Errors.NONE) ? toErrorDescription(topicMetadata.error()) : null);
    }

    private List<PartitionDescription> toPartitionDescriptions(List<MetadataResponse.PartitionMetadata> partitionMetadatas) {
        return partitionMetadatas.stream()
                .map(this::toPartitionDescription)
                .sorted(Comparator.comparing(PartitionDescription::partition))
                .collect(toList());
    }

    private PartitionDescription toPartitionDescription(MetadataResponse.PartitionMetadata partitionMetadata) {
        return new PartitionDescription()
                .withPartition(partitionMetadata.partition())
                .withLeader(partitionMetadata.leaderId())
                .withReplicas(toNodeIds(partitionMetadata.replicas()))
                .withIsr(toNodeIds(partitionMetadata.isr()))
                .withOfflineReplicas(toNodeIds(partitionMetadata.offlineReplicas()))
                .withError(!partitionMetadata.error().equals(Errors.NONE) ? toErrorDescription(partitionMetadata.error()) : null);
    }

    private List<Integer> toNodeIds(List<Node> nodes) {
        return nodes.stream()
                .map(Node::id)
                .collect(toList());
    }

    private ErrorDescription toErrorDescription(Errors error) {
        return new ErrorDescription()
                .withId((int) error.code())
                .withName(error.name())
                .withExceptionName(error.exceptionName())
                .withMessage(error.message());
    }

}
