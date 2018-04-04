package org.apache.kafka.clients.admin.metadata;

import org.apache.kafka.clients.admin.uri.Uri;
import org.apache.kafka.clients.admin.uri.ZookeeperUri;
import org.apache.kafka.clients.admin.zk.ZkKafkaClient;
import org.apache.kafka.common.requests.MetadataResponse;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ZkMetadataClient implements AutoCloseable {

    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration DEFAULT_CONNECTION_TIMEOUT = Duration.ofSeconds(60);

    private final ZkKafkaClient zkKafkaClient;

    private final BiFunction<Uri, MetadataResponse, MetadataDescription> metadataResponseMapper;

    private final ExecutorService executorService;

    public ZkMetadataClient(String zkConnectionString) {
        this(zkConnectionString, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
    }

    public ZkMetadataClient(String zkConnectionString, Duration sessionTimeout, Duration connectionTimeout) {
        this(new ZkKafkaClient(zkConnectionString, sessionTimeout, connectionTimeout, false));
    }

    public ZkMetadataClient(ZkKafkaClient zkKafkaClient) {
        this(zkKafkaClient, new MetadataResponseToMetadataDescriptionMapper(), Executors.newSingleThreadExecutor());
    }

    public ZkMetadataClient(ZkKafkaClient zkKafkaClient, BiFunction<Uri, MetadataResponse, MetadataDescription> metadataResponseMapper, ExecutorService executorService) {
        this.zkKafkaClient = requireNonNull(zkKafkaClient);
        this.metadataResponseMapper = requireNonNull(metadataResponseMapper);
        this.executorService = requireNonNull(executorService);
    }

    public Future<MetadataDescription> describe() {
        return executorService.submit(this::describeMetadata);
    }

    MetadataDescription describeMetadata() {
        Uri source = ZookeeperUri.parse("zk://" + zkKafkaClient.zkConnectionString());
        return metadataResponseMapper.apply(source, zkKafkaClient.metadataRequest());
    }

    @Override
    public void close() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(DEFAULT_SESSION_TIMEOUT.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        zkKafkaClient.close();
    }
}
