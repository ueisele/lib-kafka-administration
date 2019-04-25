package org.apache.kafka.clients.admin.controlledshutdown;

import org.apache.kafka.clients.admin.request.CompletablePromise;
import org.apache.kafka.clients.admin.request.ControlledShutdownRequestDefinition;
import org.apache.kafka.clients.admin.request.RequestClient;
import org.apache.kafka.clients.admin.request.RequestClient.NodeProvider;
import org.apache.kafka.clients.admin.request.ZkRequestClient;
import org.apache.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.toList;

public class KafkaControlledShutdownClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaControlledShutdownClient.class);

    private final RequestClient requestClient;
    private final ZkRequestClient zkRequestClient;

    private final ControlledShutdownResponseToControlledShutdownStatusMapper controlledShutdownResponseMapper;

    public KafkaControlledShutdownClient(RequestClient requestClient, ZkRequestClient zkRequestClient) {
        this(requestClient, zkRequestClient, new ControlledShutdownResponseToControlledShutdownStatusMapper());
    }

    public KafkaControlledShutdownClient(RequestClient requestClient, ZkRequestClient zkRequestClient, ControlledShutdownResponseToControlledShutdownStatusMapper controlledShutdownResponseMapper) {
        this.requestClient = requestClient;
        this.zkRequestClient = zkRequestClient;
        this.controlledShutdownResponseMapper = controlledShutdownResponseMapper;
    }
    public List<Future<ControlledShutdownStatus>> shutdown(List<Integer> brokerIds) {
            return brokerIds.stream()
                    .map(this::shutdown)
                    .collect(toList());
    }

    public Future<ControlledShutdownStatus> shutdown(int brokerId) {
        return CompletableFuture.supplyAsync(() -> zkRequestClient.brokerEpochById(brokerId))
                .thenCompose(brokerEpoch -> {
                    ControlledShutdownRequestDefinition requestDefinition = new ControlledShutdownRequestDefinition(brokerId, brokerEpoch);
                    NodeProvider nodeProvider = requestClient.toControllerNode();
                    LOG.info(String.format("Send request to %s: %s", nodeProvider, requestDefinition));
                    return new CompletablePromise<>(requestClient.request(requestDefinition, nodeProvider).nodeAndResponse());
                })
                .thenApply(pair -> controlledShutdownResponseMapper.toControlledShutdownStatus(toKafkaUri(pair.left), brokerId, pair.right));
    }

    private KafkaUri toKafkaUri(Node node) {
        return new KafkaUri(node.host(), node.port(), node.id(), node.rack());
    }

}
