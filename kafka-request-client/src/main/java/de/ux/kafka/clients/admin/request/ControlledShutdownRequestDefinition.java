package de.ux.kafka.clients.admin.request;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;

import static org.apache.kafka.common.requests.AbstractControlRequest.UNKNOWN_BROKER_EPOCH;

public class ControlledShutdownRequestDefinition extends RequestDefinition<ControlledShutdownResponse, ControlledShutdownRequestDefinition> {

    private final int shutdownBrokerId;
    private final long shutdownBrokerEpoch;

    public ControlledShutdownRequestDefinition(int shutdownBrokerId) {
        this(shutdownBrokerId, UNKNOWN_BROKER_EPOCH);
    }

    public ControlledShutdownRequestDefinition(int shutdownBrokerId, long shutdownBrokerEpoch) {
        super(ApiKeys.CONTROLLED_SHUTDOWN.name, ControlledShutdownResponse.class);
        this.shutdownBrokerId = shutdownBrokerId;
        this.shutdownBrokerEpoch = shutdownBrokerEpoch;
    }

    @Override
    AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return new Builder(shutdownBrokerId(), shutdownBrokerEpoch);
    }

    public int shutdownBrokerId() {
        return shutdownBrokerId;
    }

    public long shutdownBrokerEpoch() {
        return shutdownBrokerEpoch;
    }

    @Override
    public String toString() {
        return "ControlledShutdownRequestDefinition{" +
                "shutdownBrokerId=" + shutdownBrokerId +
                ", shutdownBrokerEpoch=" + shutdownBrokerEpoch +
                '}';
    }

    public static class Builder extends AbstractRequest.Builder<ControlledShutdownRequest> {
        private final int brokerId;
        private final long brokerEpoch;

        public Builder(int brokerId, long brokerEpoch) {
            super(ApiKeys.CONTROLLED_SHUTDOWN);
            this.brokerId = brokerId;
            this.brokerEpoch = brokerEpoch;
        }

        @Override
        public ControlledShutdownRequest build(short version) {
            Struct struct = new Struct(ApiKeys.CONTROLLED_SHUTDOWN.requestSchema(ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()));
            struct.set("broker_id", brokerId);
            struct.setIfExists("broker_epoch", brokerEpoch);
            return new ControlledShutdownRequest(struct, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ControlledShutdownRequest").
                    append(", brokerId=").append(brokerId).
                    append(", brokerEpoch=").append(brokerEpoch).
                    append(")");
            return bld.toString();
        }
    }
}
