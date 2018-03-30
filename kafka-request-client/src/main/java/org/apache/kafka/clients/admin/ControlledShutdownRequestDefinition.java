package org.apache.kafka.clients.admin;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ControlledShutdownRequest;
import org.apache.kafka.common.requests.ControlledShutdownResponse;

public class ControlledShutdownRequestDefinition extends RequestDefinition<ControlledShutdownResponse, ControlledShutdownRequestDefinition> {

    private final int shutdownBrokerId;
    private short desiredVersion = ApiKeys.CONTROLLED_SHUTDOWN.latestVersion();

    public ControlledShutdownRequestDefinition(int shutdownBrokerId) {
        super(ApiKeys.CONTROLLED_SHUTDOWN.name, ControlledShutdownResponse.class);
        this.shutdownBrokerId = shutdownBrokerId;
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return new ControlledShutdownRequest.Builder(shutdownBrokerId(), desiredVersion());
    }

    public int shutdownBrokerId() {
        return shutdownBrokerId;
    }

    public short desiredVersion() {
        return desiredVersion;
    }

    public ControlledShutdownRequestDefinition withDesiredVersion(short desiredVersion) {
        this.desiredVersion = desiredVersion;
        return this;
    }
}
