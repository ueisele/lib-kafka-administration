package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.requests.FetchRequest.PartitionData;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.requests.FetchRequest.CONSUMER_REPLICA_ID;
import static org.apache.kafka.common.requests.IsolationLevel.READ_UNCOMMITTED;

public class FetchRequestDefinition extends RequestDefinition<FetchResponse, FetchRequestDefinition> {

    public static final int DEFAULT_RESPONSE_MIN_BYTES = 1;
    public static final int DEFAULT_RESPONSE_MAX_BYTES = 52428800;

    private final Map<TopicPartition, PartitionData> fetchData;

    private short minVersion = ApiKeys.FETCH.oldestVersion();
    private short maxVersion = ApiKeys.FETCH.latestVersion();
    private int replicaId = CONSUMER_REPLICA_ID;
    private int minBytes = DEFAULT_RESPONSE_MIN_BYTES;
    private int maxBytes = DEFAULT_RESPONSE_MAX_BYTES;
    private IsolationLevel isolationLevel = READ_UNCOMMITTED;
    private FetchMetadata metadata = FetchMetadata.LEGACY;
    private List<TopicPartition> toForget = emptyList();

    public FetchRequestDefinition(Map<TopicPartition, PartitionData> fetchData) {
        super(ApiKeys.FETCH.name, FetchResponse.class);
        this.fetchData = fetchData;
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return new FetchRequest.Builder(minVersion, maxVersion, replicaId, (int) timeoutMs, minBytes, fetchData);
    }

    public Map<TopicPartition, PartitionData> fetchData() {
        return fetchData;
    }

    public short minVersion() {
        return minVersion;
    }

    public FetchRequestDefinition withMinVersion(short minVersion) {
        this.minVersion = minVersion;
        return this;
    }

    public short maxVersion() {
        return maxVersion;
    }

    public FetchRequestDefinition withMaxVersion(short maxVersion) {
        this.maxVersion = maxVersion;
        return this;
    }

    public int replicaId() {
        return replicaId;
    }

    public FetchRequestDefinition withConsumerReplicaId() {
        return withReplicaId(CONSUMER_REPLICA_ID);
    }

    public FetchRequestDefinition withReplicaId(int replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public int minBytes() {
        return minBytes;
    }

    public FetchRequestDefinition withMinBytes(int minBytes) {
        this.minBytes = minBytes;
        return this;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public FetchRequestDefinition withMaxBytes(int maxBytes) {
        this.maxBytes = maxBytes;
        return this;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public FetchRequestDefinition withIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        return this;
    }

    public FetchMetadata metadata() {
        return metadata;
    }

    public FetchRequestDefinition withMetadata(FetchMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public List<TopicPartition> toForget() {
        return toForget;
    }

    public FetchRequestDefinition withToForget(List<TopicPartition> toForget) {
        this.toForget = toForget;
        return this;
    }
}
