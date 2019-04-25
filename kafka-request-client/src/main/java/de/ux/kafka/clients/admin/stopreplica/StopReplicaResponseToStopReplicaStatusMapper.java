package de.ux.kafka.clients.admin.stopreplica;

import de.ux.kafka.clients.admin.metadata.ErrorDescription;
import de.ux.kafka.clients.admin.uri.KafkaUri;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StopReplicaResponse;

import java.util.Map;
import java.util.stream.Collectors;

public class StopReplicaResponseToStopReplicaStatusMapper {

    public StopReplicaStatus toStopReplicaStatus(KafkaUri source, StopReplicaResponse stopReplicaResponse) {
        return new StopReplicaStatus()
                .withSource(source)
                .withError(!Errors.NONE.equals(stopReplicaResponse.error()) ? toErrorDescription(stopReplicaResponse.error()) : null)
                .withPartitions(toPartitionStates(stopReplicaResponse.responses()));
    }

    private ErrorDescription toErrorDescription(Errors error) {
        return new ErrorDescription()
                .withId((int) error.code())
                .withName(error.name())
                .withExceptionName(error.exceptionName())
                .withMessage(error.message());
    }

    private Map<TopicPartition, ErrorDescription> toPartitionStates(Map<TopicPartition, Errors> partitionResponses) {
        return partitionResponses.entrySet().stream()
                .map(entry -> ImmutablePair.of(entry.getKey(), toErrorDescription(entry.getValue())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

}
