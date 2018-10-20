package org.apache.kafka.clients.admin.controlledshutdown;

import org.apache.kafka.clients.admin.request.ControlledShutdownRequestDefinition;
import org.apache.kafka.clients.admin.request.RequestClient;
import org.apache.kafka.clients.admin.request.ResponseResult;
import org.apache.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.ControlledShutdownResponse;

import java.util.List;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;

public class KafkaControlledShutdownClient {

    private final RequestClient requestClient;

    private final ControlledShutdownResponseToControlledShutdownStatusMapper controlledShutdownResponseMapper;

    public KafkaControlledShutdownClient(RequestClient requestClient) {
        this(requestClient, new ControlledShutdownResponseToControlledShutdownStatusMapper());
    }

    public KafkaControlledShutdownClient(RequestClient requestClient, ControlledShutdownResponseToControlledShutdownStatusMapper controlledShutdownResponseMapper) {
        this.requestClient = requestClient;
        this.controlledShutdownResponseMapper = controlledShutdownResponseMapper;
    }
    public List<Future<ControlledShutdownStatus>> shutdown(List<Integer> brokerIds) {
            return brokerIds.stream()
                    .map(this::shutdown)
                    .collect(toList());
    }

    public Future<ControlledShutdownStatus> shutdown(int brokerId) {
        ResponseResult<ControlledShutdownResponse> responseResult =
                requestClient.request(new ControlledShutdownRequestDefinition(brokerId), requestClient.toControllerNode());
        return responseResult.nodeAndResponse().thenApply(pair -> controlledShutdownResponseMapper.toControlledShutdownStatus(toKafkaUri(pair.left), brokerId, pair.right));
    }

    private KafkaUri toKafkaUri(Node node) {
        return new KafkaUri(node.host(), node.port(), node.id(), node.rack());
    }

}
