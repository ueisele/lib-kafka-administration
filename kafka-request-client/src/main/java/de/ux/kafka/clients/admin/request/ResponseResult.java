package de.ux.kafka.clients.admin.request;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.Map;

public class ResponseResult<T extends AbstractResponse> {

    final KafkaFuture<ImmutablePair<Node, T>> future;

    ResponseResult(KafkaFuture<ImmutablePair<Node, T>> future) {
        this.future = future;
    }

    public KafkaFuture<ImmutablePair<Node, T>> nodeAndResponse() {
        return future;
    }

    public KafkaFuture<Node> node() {
        return future.thenApply(ImmutablePair::getLeft);
    }

    public KafkaFuture<T> response() {
        return future.thenApply(ImmutablePair::getRight);
    }

    public KafkaFuture<Map<Errors, Integer>> errorCounts() {
        return response().thenApply(AbstractResponse::errorCounts);
    }

    public KafkaFuture<Boolean> hasErrors() {
        return errorCounts().thenApply(errorCounts -> !errorCounts.isEmpty());
    }
}
