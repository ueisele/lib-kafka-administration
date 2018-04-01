package org.apache.kafka.clients.admin.uri;

public class UriSyntaxException extends RuntimeException {

    public UriSyntaxException(String message) {
        super(message);
    }

    public UriSyntaxException(String message, Throwable cause) {
        super(message, cause);
    }
}
