package org.apache.kafka.clients.admin.request;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.StopReplicaRequest;
import org.apache.kafka.common.requests.StopReplicaResponse;

import java.util.HashSet;
import java.util.Set;

import static org.apache.kafka.common.requests.AbstractControlRequest.UNKNOWN_BROKER_EPOCH;

public class StopReplicaRequestDefinition extends RequestDefinition<StopReplicaResponse, StopReplicaRequestDefinition> {

    private static final ApiKeys API_KEY = ApiKeys.STOP_REPLICA;

    private final int controllerId = -1;
    private final int controllerEpoch = Integer.MAX_VALUE;
    private final long brokerEpoch = UNKNOWN_BROKER_EPOCH;

    private final Set<TopicPartition> partitions;

    public StopReplicaRequestDefinition(Set<TopicPartition> partitions) {
        super(API_KEY.name, StopReplicaResponse.class);
        this.partitions = new HashSet<>(partitions);
    }

    @Override
    AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return new StopReplicaRequest.Builder(API_KEY.latestVersion(), controllerId, controllerEpoch, brokerEpoch, false, partitions);
    }
}
