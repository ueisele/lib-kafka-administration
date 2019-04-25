package de.ux.kafka.clients.admin.stopreplica;

import de.ux.kafka.clients.admin.request.RequestClient;
import de.ux.kafka.clients.admin.request.ResponseResult;
import de.ux.kafka.clients.admin.request.StopReplicaRequestDefinition;
import de.ux.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.StopReplicaResponse;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;

public class KafkaStopReplicaClient {

    private final RequestClient requestClient;

    private final StopReplicaResponseToStopReplicaStatusMapper stopReplicaResponseMapper;

    public KafkaStopReplicaClient(RequestClient requestClient) {
        this(requestClient, new StopReplicaResponseToStopReplicaStatusMapper());
    }

    public KafkaStopReplicaClient(RequestClient requestClient, StopReplicaResponseToStopReplicaStatusMapper stopReplicaResponseMapper) {
        this.requestClient = requestClient;
        this.stopReplicaResponseMapper = stopReplicaResponseMapper;
    }
    public List<Future<StopReplicaStatus>> stopReplica(List<Integer> brokerIds, Set<TopicPartition> topicPartitions) {
            return brokerIds.stream()
                    .map(brokerId -> stopReplica(brokerId, topicPartitions))
                    .collect(toList());
    }

    public Future<StopReplicaStatus> stopReplica(int brokerId, Set<TopicPartition> topicPartitions) {
        ResponseResult<StopReplicaResponse> responseResult =
                requestClient.request(new StopReplicaRequestDefinition(topicPartitions), requestClient.toNode(brokerId));
        return responseResult.nodeAndResponse().thenApply(pair -> stopReplicaResponseMapper.toStopReplicaStatus(toKafkaUri(pair.left), pair.right));
    }

    private KafkaUri toKafkaUri(Node node) {
        return new KafkaUri(node.host(), node.port(), node.id(), node.rack());
    }

}
