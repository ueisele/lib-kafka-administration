package de.ux.kafka.clients.admin.request;

import de.ux.kafka.clients.admin.zk.KafkaZkMetadataClient;

import java.time.Duration;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.requests.AbstractControlRequest.UNKNOWN_BROKER_EPOCH;

public class ZkRequestClient implements AutoCloseable {

    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);

    private final KafkaZkMetadataClient zkKafkaClient;

    public ZkRequestClient(String zkConnectionString) {
        this(zkConnectionString, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
    }

    public ZkRequestClient(String zkConnectionString, Duration sessionTimeout, Duration connectionTimeout) {
        this(new KafkaZkMetadataClient(zkConnectionString, sessionTimeout, connectionTimeout, false));
    }

    public ZkRequestClient(KafkaZkMetadataClient zkKafkaClient) {
        this.zkKafkaClient = requireNonNull(zkKafkaClient);
    }

    public long brokerEpochById(int brokerId) {
        if(zkKafkaClient.getBrokerEpoch(brokerId).nonEmpty()) {
            return (Long)zkKafkaClient.getBrokerEpoch(brokerId).get();
        }
        return UNKNOWN_BROKER_EPOCH;
    }

    @Override
    public void close() {
        zkKafkaClient.close();
    }
}
