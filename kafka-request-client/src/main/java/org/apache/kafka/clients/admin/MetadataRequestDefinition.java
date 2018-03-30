package org.apache.kafka.clients.admin;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.emptyList;

public class MetadataRequestDefinition extends RequestDefinition<MetadataResponse, MetadataRequestDefinition> {

    public static final List<String> ALL_TOPICS = null;
    public static final List<String> NO_TOPICS = emptyList();

    public static final List<String> DEFAULT_TOPIC_NAMES = ALL_TOPICS;
    public static final boolean DEFAULT_ALLOW_AUTO_TOPIC_CREATION = true;

    private List<String> topicNames = DEFAULT_TOPIC_NAMES;
    private boolean allowAutoTopicCreation = DEFAULT_ALLOW_AUTO_TOPIC_CREATION;

    public MetadataRequestDefinition() {
        super(ApiKeys.METADATA.name, MetadataResponse.class);
    }

    @Override
    public AbstractRequest.Builder<?> requestBuilder(long timeoutMs) {
        return new MetadataRequest.Builder(new ArrayList<>(topicNames), allowAutoTopicCreation);
    }

    public Collection<String> topicNames() {
        return topicNames;
    }

    public MetadataRequestDefinition withAllTopics() {
        return withTopicNames(ALL_TOPICS);
    }

    public MetadataRequestDefinition withoutTopics() {
        return withTopicNames(NO_TOPICS);
    }

    public MetadataRequestDefinition withTopicNames(List<String> topicNames) {
        this.topicNames = topicNames;
        return this;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    public MetadataRequestDefinition withAllowAutoTopicCreation(boolean allowAutoTopicCreation) {
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        return this;
    }
}
