package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.Map;

public class ProduceRequestDefinition extends RequestDefinition<ProduceResponse, ProduceRequestDefinition> {

    public static final short ACKS_ALL = -1;

    public static final short DEFAULT_ACKS = 1;

    private final Map<TopicPartition, MemoryRecords> partitionRecords;

    private byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
    private short acks = DEFAULT_ACKS;
    private String transactionalId = null;

    public ProduceRequestDefinition(Map<TopicPartition, MemoryRecords> partitionRecords) {
        super(ApiKeys.PRODUCE.name, ProduceResponse.class);
        this.partitionRecords = partitionRecords;
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return ProduceRequest.Builder.forMagic(magic, acks, (int) timeoutMs, partitionRecords, transactionalId);
    }

    public Map<TopicPartition, MemoryRecords> partitionRecords() {
        return partitionRecords;
    }

    public byte magic() {
        return magic;
    }

    public ProduceRequestDefinition withMagicByVersion(short desiredVersion) {
        return withAcks(ProduceRequest.requiredMagicForVersion(desiredVersion));
    }

    public ProduceRequestDefinition withMagic(byte magic) {
        this.magic = magic;
        return this;
    }

    public short acks() {
        return acks;
    }

    public ProduceRequestDefinition withAcksAll() {
        return withAcks(ACKS_ALL);
    }

    public ProduceRequestDefinition withAcks(short acks) {
        this.acks = acks;
        return this;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public ProduceRequestDefinition withTransactionalId(String transactionalId) {
        this.transactionalId = transactionalId;
        return this;
    }
}
