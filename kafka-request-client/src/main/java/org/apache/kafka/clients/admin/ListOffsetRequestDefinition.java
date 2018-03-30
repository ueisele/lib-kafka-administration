package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.String.format;
import static org.apache.kafka.common.requests.IsolationLevel.READ_UNCOMMITTED;
import static org.apache.kafka.common.requests.ListOffsetRequest.CONSUMER_REPLICA_ID;

public class ListOffsetRequestDefinition extends RequestDefinition<ListOffsetResponse, ListOffsetRequestDefinition> {

    private final Map<TopicPartition, Long> partitionTimestamps;

    private short minVersion = ApiKeys.LIST_OFFSETS.oldestVersion();
    private short maxVersion = ApiKeys.LIST_OFFSETS.latestVersion();
    private int replicaId = CONSUMER_REPLICA_ID;
    private IsolationLevel isolationLevel = READ_UNCOMMITTED;

    public ListOffsetRequestDefinition(Map<TopicPartition, Long> partitionTimestamps) {
        super(ApiKeys.LIST_OFFSETS.name, ListOffsetResponse.class);
        this.partitionTimestamps = partitionTimestamps;
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        Constructor<ListOffsetRequest.Builder> builderConstructor;
        try {
            builderConstructor = ListOffsetRequest.Builder.class.getConstructor(short.class, short.class, int.class, IsolationLevel.class);
        } catch (NoSuchMethodException e) {
            throw new RequestBuilderException(format("Could not create request builder for %s.", this.getClass().getSimpleName()), e);
        }
        builderConstructor.setAccessible(true);
        try {
            return builderConstructor.newInstance((short) max(minVersion, minVersionBy(isolationLevel)), maxVersion, replicaId, isolationLevel);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RequestBuilderException(format("Could not create request builder for %s.", this.getClass().getSimpleName()), e);
        }
    }

    public static short minVersionBy(IsolationLevel isolationLevel) {
        short minVersion = ApiKeys.LIST_OFFSETS.oldestVersion();
        if (isolationLevel == IsolationLevel.READ_COMMITTED)
            minVersion = (short) max(minVersion, 2);
        return minVersion;
    }

    public Map<TopicPartition, Long> partitionTimestamps() {
        return partitionTimestamps;
    }

    public short minVersion() {
        return minVersion;
    }

    public ListOffsetRequestDefinition withMinVersionByRequireTimestamp(boolean requireTimestamp) {
        short minVersion = ApiKeys.LIST_OFFSETS.oldestVersion();
        if (requireTimestamp)
            minVersion = (short) max(minVersion, 1);
        return withMinVersion(minVersion);
    }

    public ListOffsetRequestDefinition withMinVersion(short minVersion) {
        this.minVersion = minVersion;
        return this;
    }

    public short maxVersion() {
        return maxVersion;
    }

    public ListOffsetRequestDefinition withMaxVersion(short maxVersion) {
        this.maxVersion = maxVersion;
        return this;
    }

    public int replicaId() {
        return replicaId;
    }

    public ListOffsetRequestDefinition withConsumerReplicaId() {
        return withReplicaId(CONSUMER_REPLICA_ID);
    }

    public ListOffsetRequestDefinition withReplicaId(int replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public ListOffsetRequestDefinition withIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }
}
