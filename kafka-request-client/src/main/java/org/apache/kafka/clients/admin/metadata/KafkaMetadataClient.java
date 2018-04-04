package org.apache.kafka.clients.admin.metadata;

import org.apache.kafka.clients.admin.request.MetadataRequestDefinition;
import org.apache.kafka.clients.admin.request.RequestClient;
import org.apache.kafka.clients.admin.request.RequestClient.NodeProvider;
import org.apache.kafka.clients.admin.request.ResponseResult;
import org.apache.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.clients.admin.uri.Uri;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class KafkaMetadataClient {

    private final RequestClient requestClient;

    private final BiFunction<Uri, MetadataResponse, MetadataDescription> metadataResponseMapper;

    public KafkaMetadataClient(RequestClient requestClient) {
        this(requestClient, new MetadataResponseToMetadataDescriptionMapper());
    }

    public KafkaMetadataClient(RequestClient requestClient, BiFunction<Uri, MetadataResponse, MetadataDescription> metadataResponseMapper) {
        this.requestClient = requestClient;
        this.metadataResponseMapper = metadataResponseMapper;
    }

    public List<Future<MetadataDescription>> describe(List<NodeProvider> nodeProviders) {
            return nodeProviders.stream()
                    .map(this::describe)
                    .collect(toList());
    }

    public Future<MetadataDescription> describe(NodeProvider nodeProvider) {
        ResponseResult<MetadataResponse> responseResult = requestClient.request(
                new MetadataRequestDefinition().withAllTopics().withAllowAutoTopicCreation(false),
                nodeProvider);
        return responseResult.nodeAndResponse().thenApply(pair -> metadataResponseMapper.apply(toKafkaUri(pair.left), pair.right));
    }

    private KafkaUri toKafkaUri(Node node) {
        return new KafkaUri(node.host(), node.port(), node.id(), node.rack());
    }

    public List<NodeProvider> atAllNodes() {
        return requestClient.toAllNodes();
    }

    public List<NodeProvider> atNodes(Integer... nodeIds) {
        return stream(nodeIds)
                .map(this::atNode)
                .collect(toList());
    }

    public NodeProvider atNode(String host, int port) {
        return requestClient.toNode(host, port);
    }

    public NodeProvider atNode(int nodeId) {
        return requestClient.toNode(nodeId);
    }

    public NodeProvider atControllerNode() {
        return requestClient.toControllerNode();
    }

    public NodeProvider atAnyNode() {
        return requestClient.toLeastLoadedNode();
    }

}
