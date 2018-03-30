package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.Map;

public class ResponseResult<T extends AbstractResponse> {

    final KafkaFuture<T> future;

    ResponseResult(KafkaFuture<T> future) {
        this.future = future;
    }

    public KafkaFuture<T> response() {
        return future;
    }

    public KafkaFuture<Map<Errors, Integer>> errorCounts() {
        return future.thenApply(AbstractResponse::errorCounts);
    }

    public KafkaFuture<Boolean> hasErrors() {
        return errorCounts().thenApply(errorCounts -> !errorCounts.isEmpty());
    }
}
