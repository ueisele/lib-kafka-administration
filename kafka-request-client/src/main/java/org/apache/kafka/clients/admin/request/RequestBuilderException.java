package org.apache.kafka.clients.admin.request;

public class RequestBuilderException extends RuntimeException {

    public RequestBuilderException(String message) {
        super(message);
    }

    public RequestBuilderException(String message, Throwable cause) {
        super(message, cause);
    }
}
