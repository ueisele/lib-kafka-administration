package org.apache.kafka.clients.admin.metadata;

import org.apache.kafka.clients.admin.request.RequestClient;

public class MetadataClient {

    private final RequestClient requestClient;

    public MetadataClient(RequestClient requestClient) {
        this.requestClient = requestClient;
    }


}
