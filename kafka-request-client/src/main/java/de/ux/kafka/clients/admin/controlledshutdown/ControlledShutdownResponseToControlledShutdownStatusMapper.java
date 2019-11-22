package de.ux.kafka.clients.admin.controlledshutdown;

import de.ux.kafka.clients.admin.metadata.ErrorDescription;
import de.ux.kafka.clients.admin.uri.KafkaUri;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ControlledShutdownResponse;

import static java.util.stream.Collectors.toSet;

public class ControlledShutdownResponseToControlledShutdownStatusMapper {

    public ControlledShutdownStatus toControlledShutdownStatus(KafkaUri source, int shutdownBrokerId, ControlledShutdownResponse controlledShutdownResponse) {
        return new ControlledShutdownStatus()
                .withSource(source)
                .withShutdownBrokerId(shutdownBrokerId)
                .withError(!Errors.NONE.equals(controlledShutdownResponse.error()) ? toErrorDescription(controlledShutdownResponse.error()) : null)
                .withPartitionsRemaining(controlledShutdownResponse.data().remainingPartitions().stream()
                        .map(r -> new TopicPartition(r.topicName(), r.partitionIndex()))
                        .collect(toSet()));

    }

    private ErrorDescription toErrorDescription(Errors error) {
        return new ErrorDescription()
                .withId((int) error.code())
                .withName(error.name())
                .withExceptionName(error.exceptionName())
                .withMessage(error.message());
    }

}
