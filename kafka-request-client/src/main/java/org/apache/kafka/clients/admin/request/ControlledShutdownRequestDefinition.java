package org.apache.kafka.clients.admin.request;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;

import static org.apache.kafka.common.requests.AbstractControlRequest.UNKNOWN_BROKER_EPOCH;

public class ControlledShutdownRequestDefinition extends RequestDefinition<ControlledShutdownResponse, ControlledShutdownRequestDefinition> {

    private final int shutdownBrokerId;

    public ControlledShutdownRequestDefinition(int shutdownBrokerId) {
        super(ApiKeys.CONTROLLED_SHUTDOWN.name, ControlledShutdownResponse.class);
        this.shutdownBrokerId = shutdownBrokerId;
    }

    @Override
    AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        //see KafkaZkClient.getAllBrokerAndEpochsInCluster();
        //Broker Epoch is Czxid in ZK entry of broker
        return new Builder(shutdownBrokerId(), UNKNOWN_BROKER_EPOCH);
    }

    public int shutdownBrokerId() {
        return shutdownBrokerId;
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
