/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

public class RequestClient implements AutoCloseable {

    /**
     * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The prefix to use for the JMX metrics for this class
     */
    private static final String JMX_PREFIX = "kafka.admin.client";

    /**
     * An invalid shutdown time which indicates that a shutdown has not yet been performed.
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    private final Logger log;

    /**
     * The default timeout to use for an operation.
     */
    private final int defaultTimeoutMs;

    /**
     * The name of this AdminClient instance.
     */
    private final String clientId;

    /**
     * Provides the time.
     */
    private final Time time;

    /**
     * The cluster metadata used by the KafkaClient.
     */
    private final Metadata metadata;

    /**
     * The metrics for this KafkaAdminClient.
     */
    private final Metrics metrics;

    /**
     * The network client to use.
     */
    private final KafkaClient client;

    /**
     * The runnable used in the service thread for this admin client.
     */
    private final AdminClientRunnable runnable;

    /**
     * The network service thread for this admin client.
     */
    private final Thread thread;

    /**
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * A factory which creates TimeoutProcessors for the RPC thread.
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    private final int maxRetries;

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        List<V> list = map.get(key);
        if (list != null)
            return list;
        list = new LinkedList<>();
        map.put(key, list);
        return list;
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    static String generateClientId(AdminClientConfig config) {
        String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
        if (!clientId.isEmpty())
            return clientId;
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    /**
     * Pretty-print an exception.
     *
     * @param throwable     The exception.
     *
     * @return              A compact human-readable string.
     */
    static String prettyPrintException(Throwable throwable) {
        if (throwable == null)
            return "Null exception.";
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        return throwable.getClass().getSimpleName();
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new KafkaAdminClient.
     */
    public static RequestClient create(Properties props) {
        return createInternal(new AdminClientConfig(props), null);
    }

    /**
     * Create a new AdminClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new KafkaAdminClient.
     */
    public static RequestClient create(Map<String, Object> conf) {
        return createInternal(new AdminClientConfig(conf), null);
    }

    static RequestClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ChannelBuilder channelBuilder = null;
        Selector selector = null;
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);

        try {
            // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
            // simplifies communication with older brokers)
            Metadata metadata = new Metadata(config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG), true);
            List<MetricsReporter> reporters = config.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            reporters.add(new JmxReporter(JMX_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                selector,
                metadata,
                clientId,
                1,
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                (int) TimeUnit.HOURS.toMillis(1),
                time,
                true,
                apiVersions,
                logContext);
            return new RequestClient(config, clientId, time, metadata, metrics, networkClient,
                timeoutProcessorFactory, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed create new KafkaAdminClient", exc);
        }
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    private RequestClient(AdminClientConfig config, String clientId, Time time, Metadata metadata,
                          Metrics metrics, KafkaClient client, TimeoutProcessorFactory timeoutProcessorFactory,
                          LogContext logContext) {
        this.defaultTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.time = time;
        this.metadata = metadata;
        List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
            config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());
        this.metrics = metrics;
        this.client = client;
        this.runnable = new AdminClientRunnable();
        String threadName = "kafka-admin-client-thread | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
            new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    public <T extends AbstractResponse> ResponseResult<T> request(final RequestDefinition<T, ?> requestDefinition, final NodeProvider nodeProvider) {
        final KafkaFutureImpl<T> responseFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new RequestResponseCall<>(requestDefinition, nodeProvider, calcDeadlineMs(now, requestDefinition.timeoutMs()), responseFuture), now);
        return new ResponseResult<>(responseFuture);
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        return now + defaultTimeoutMs;
    }

    /**
     * Close the AdminClient and release all associated resources.
     *
     * See {@link AdminClient#close(long, TimeUnit)}
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public void close(long duration, TimeUnit unit) {
        long waitTimeMs = unit.toMillis(duration);
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                client.wakeup(); // Wake the thread, if it is blocked inside poll().
                break;
            }
            prev = hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        try {
            // Wait for the thread to be joined.
            thread.join();

            AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

            log.debug("Kafka admin client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    public interface NodeProvider {
        Node provide();
    }

    public class DirectNodeProvider implements NodeProvider {
        private final Node node;

        public DirectNodeProvider(int id, String host, int port) {
            this(new Node(id, host, port));
        }

        public DirectNodeProvider(int id, String host, int port, String rack) {
            this(new Node(id, host, port, rack));
        }

        public DirectNodeProvider(Node node) {
            this.node = node;
        }

        @Override
        public Node provide() {
            return node;
        }
    }

    public class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId;

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            return metadata.fetch().nodeById(nodeId);
        }
    }

    /**
     * Provides the controller node.
     */
    public class ControllerNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            return metadata.fetch().controller();
        }
    }

    /**
     * Provides the least loaded node.
     */
    public class LeastLoadedNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            return client.leastLoadedNode(time.milliseconds());
        }
    }

    public class LeaderForTopicPartitionNodeProvider implements NodeProvider {
        private final TopicPartition topicPartition;

        public LeaderForTopicPartitionNodeProvider(TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
        }

        @Override
        public Node provide() {
            return metadata.fetch().leaderFor(topicPartition);
        }
    }

    class RequestResponseCall<T extends AbstractResponse> extends Call {

        private final RequestDefinition<T, ?> requestDefinition;
        private final KafkaFutureImpl<T> responseFuture;

        RequestResponseCall(RequestDefinition<T, ?> requestDefinition, NodeProvider nodeProvider, long deadlineMs, KafkaFutureImpl<T> responseFuture) {
            super(requestDefinition.requestName(), deadlineMs, nodeProvider);
            this.requestDefinition = requestDefinition;
            this.responseFuture = responseFuture;
        }

        @Override
        AbstractRequest.Builder createRequest(int timeoutMs) {
            return requestDefinition.requestBuilder(timeoutMs);
        }

        @Override
        void handleResponse(AbstractResponse abstractResponse) {
            responseFuture.complete(requestDefinition.response(abstractResponse));
        }

        @Override
        void handleFailure(Throwable throwable) {
            responseFuture.completeExceptionally(throwable);
        }
    }

    abstract class Call {
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now           The current time in milliseconds.
         * @param throwable     The failure exception.
         */
        final void fail(long now, Throwable throwable) {
            if (aborted) {
                // If the call was aborted while in flight due to a timeout, deliver a
                // TimeoutException. In this case, we do not get any more retries - the call has
                // failed. We increment tries anyway in order to display an accurate log message.
                tries++;
                if (log.isDebugEnabled()) {
                    log.debug("{} aborted at {} after {} attempt(s)", this, now, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(new TimeoutException("Aborted due to timeout."));
                return;
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.trace("{} attempting protocol downgrade.", this);
                runnable.enqueue(this, now);
                return;
            }
            tries++;
            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If the exception is not retryable, fail.
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries);
            }
            runnable.enqueue(this, now);
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         *
         * @return          The AbstractRequest builder.
         */
        abstract AbstractRequest.Builder createRequest(int timeoutMs);

        /**
         * Process the call response.
         *
         * @param abstractResponse  The AbstractResponse.
         *
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure. This will only be called if the failure exception was not
         * retryable, or if we hit a timeout.
         *
         * @param throwable     The exception.
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception     The exception.
         *
         * @return              True if the exception can be handled; false otherwise.
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs + ")";
        }
    }

    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    static class TimeoutProcessor {
        /**
         * The current time in milliseconds.
         */
        private final long now;

        /**
         * The number of milliseconds until the next timeout.
         */
        private int nextTimeoutMs;

        /**
         * Create a new timeout processor.
         *
         * @param now           The current time in milliseconds since the epoch.
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(now, new TimeoutException(msg));
                    iter.remove();
                    numTimedOut++;
                } else {
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call      The call.
         *
         * @return          True if the call should be timed out.
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    private final class AdminClientRunnable implements Runnable {
        /**
         * Pending calls. Protected by the object monitor.
         * This will be null only if the thread has shut down.
         */
        private List<Call> newCalls = new LinkedList<>();

        /**
         * Check if the AdminClient metadata is ready.
         * We need to know who the controller is, and have a non-empty view of the cluster.
         *
         * @param prevMetadataVersion       The previous metadata version which wasn't usable.
         * @return                          null if the metadata is usable; the current metadata
         *                                  version otherwise
         */
        private Integer checkMetadataReady(Integer prevMetadataVersion) {
            if (prevMetadataVersion != null) {
                if (prevMetadataVersion == metadata.version())
                    return prevMetadataVersion;
            }
            Cluster cluster = metadata.fetch();
            if (cluster.nodes().isEmpty()) {
                log.trace("Metadata is not ready yet. No cluster nodes found.");
                return metadata.requestUpdate();
            }
            if (cluster.controller() == null) {
                log.trace("Metadata is not ready yet. No controller found.");
                return metadata.requestUpdate();
            }
            if (prevMetadataVersion != null) {
                log.trace("Metadata is now ready.");
            }
            return null;
        }

        /**
         * Time out the elements in the newCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private synchronized void timeoutNewCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(newCalls,
                    "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} new calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         */
        private void timeoutCallsToSend(TimeoutProcessor processor, Map<Node, List<Call>> callsToSend) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                    "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
        }

        /**
         * Choose nodes for the calls in the callsToSend list.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         *
         */
        private void chooseNodesForNewCalls(long now, Map<Node, List<Call>> callsToSend) {
            List<Call> newCallsToAdd = null;
            synchronized (this) {
                if (newCalls.isEmpty()) {
                    return;
                }
                newCallsToAdd = newCalls;
                newCalls = new LinkedList<>();
            }
            for (Call call : newCallsToAdd) {
                chooseNodeForNewCall(now, callsToSend, call);
            }
        }

        /**
         * Choose a node for a new call.
         *
         * @param now           The current time in milliseconds.
         * @param callsToSend   A map of nodes to the calls they need to handle.
         * @param call          The call.
         */
        private void chooseNodeForNewCall(long now, Map<Node, List<Call>> callsToSend, Call call) {
            Node node = call.nodeProvider.provide();
            if (node == null) {
                call.fail(now, new BrokerNotAvailableException(
                    String.format("Error choosing node for %s: no node found.", call.callName)));
                return;
            }
            log.trace("Assigned {} to {}", call, node);
            getOrCreateListValue(callsToSend, node).add(call);
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @param callsToSend           The calls to send, by node.
         * @param correlationIdToCalls  A map of correlation IDs to calls.
         * @param callsInFlight         A map of nodes to the calls they have in flight.
         *
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now, Map<Node, List<Call>> callsToSend,
                         Map<Integer, Call> correlationIdToCalls, Map<String, List<Call>> callsInFlight) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator();
                 iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (!client.ready(node, now)) {
                    long nodeTimeout = client.connectionDelay(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                Call call = calls.remove(0);
                int timeoutMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                AbstractRequest.Builder<?> requestBuilder = null;
                try {
                    requestBuilder = call.createRequest(timeoutMs);
                } catch (Throwable throwable) {
                    call.fail(now, new KafkaException(String.format(
                        "Internal error sending %s to %s.", call.callName, node)));
                    continue;
                }
                ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true);
                log.trace("Sending {} to {}. correlationId={}", requestBuilder, node, clientRequest.correlationId());
                client.send(clientRequest, now);
                getOrCreateListValue(callsInFlight, node.idString()).add(call);
                correlationIdToCalls.put(clientRequest.correlationId(), call);
            }
            return pollTimeout;
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They may
         * even be in the process of being processed by the remote server. At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param processor         The timeout processor.
         * @param callsInFlight     A map of nodes to the calls they have in flight.
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor, Map<String, List<Call>> callsInFlight) {
            int numTimedOut = 0;
            for (Map.Entry<String, List<Call>> entry : callsInFlight.entrySet()) {
                List<Call> contexts = entry.getValue();
                if (contexts.isEmpty())
                    continue;
                String nodeId = entry.getKey();
                // We assume that the first element in the list is the earliest. So it should be the
                // only one we need to check the timeout for.
                Call call = contexts.get(0);
                if (processor.callHasExpired(call)) {
                    if (call.aborted) {
                        log.warn("Aborted call {} is still in callsInFlight.", call);
                    } else {
                        log.debug("Closing connection to {} to time out {}", nodeId, call);
                        call.aborted = true;
                        client.disconnect(nodeId);
                        numTimedOut++;
                        // We don't remove anything from the callsInFlight data structure. Because the connection
                        // has been closed, the calls should be returned by the next client#poll(),
                        // and handled at that point.
                    }
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * If an authentication exception is encountered with connection to any broker,
         * fail all pending requests.
         */
        private void handleAuthenticationException(long now, Map<Node, List<Call>> callsToSend) {
            AuthenticationException authenticationException = metadata.getAndClearAuthenticationException();
            if (authenticationException == null) {
                for (Node node : callsToSend.keySet()) {
                    authenticationException = client.authenticationException(node);
                    if (authenticationException != null)
                        break;
                }
            }
            if (authenticationException != null) {
                synchronized (this) {
                    failCalls(now, newCalls, authenticationException);
                }
                for (List<Call> calls : callsToSend.values()) {
                    failCalls(now, calls, authenticationException);
                }
                callsToSend.clear();
            }
        }

        private void failCalls(long now, List<Call> calls, AuthenticationException authenticationException) {
            for (Call call : calls) {
                call.fail(now, authenticationException);
            }
            calls.clear();
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         * @param correlationIdToCall   A map of correlation IDs to calls.
         * @param callsInFlight         A map of nodes to the calls they have in flight.
        **/
        private void handleResponses(long now, List<ClientResponse> responses, Map<String, List<Call>> callsInFlight,
                                     Map<Integer, Call> correlationIdToCall) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCall.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                        response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToCall.remove(correlationId);
                List<Call> calls = callsInFlight.get(response.destination());
                if ((calls == null) || (!calls.remove(call))) {
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                        "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
                // retryible exception.
                if (response.versionMismatch() != null) {
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    call.fail(now, new DisconnectException(String.format(
                        "Cancelled %s request with correlation id %s due to node %s being disconnected",
                        call.callName, correlationId, response.destination())));
                } else {
                    try {
                        call.handleResponse(response.responseBody());
                        if (log.isTraceEnabled())
                            log.trace("{} got response {}", call,
                                    response.responseBody().toString(response.requestHeader().apiVersion()));
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        private synchronized boolean threadShouldExit(long now, long curHardShutdownTimeMs,
                                                      Map<Node, List<Call>> callsToSend, Map<Integer, Call> correlationIdToCalls) {
            if (newCalls.isEmpty() && callsToSend.isEmpty() && correlationIdToCalls.isEmpty()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now > curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            /*
             * Maps nodes to calls that we want to send.
             */
            Map<Node, List<Call>> callsToSend = new HashMap<>();

            /*
             * Maps node ID strings to calls that have been sent.
             */
            Map<String, List<Call>> callsInFlight = new HashMap<>();

            /*
             * Maps correlation IDs to calls that have been sent.
             */
            Map<Integer, Call> correlationIdToCalls = new HashMap<>();

            /*
             * The previous metadata version which wasn't usable, or null if there is none.
             */
            Integer prevMetadataVersion = null;

            long now = time.milliseconds();
            log.trace("Thread starting");
            while (true) {
                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) &&
                        threadShouldExit(now, curHardShutdownTimeMs, callsToSend, correlationIdToCalls))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutNewCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor, callsToSend);
                timeoutCallsInFlight(timeoutProcessor, callsInFlight);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Handle new calls and metadata update requests.
                prevMetadataVersion = checkMetadataReady(prevMetadataVersion);
                if (prevMetadataVersion == null) {
                    chooseNodesForNewCalls(now, callsToSend);
                    pollTimeout = Math.min(pollTimeout,
                        sendEligibleCalls(now, callsToSend, correlationIdToCalls, callsInFlight));
                }

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleAuthenticationException(now, callsToSend);
                handleResponses(now, responses, callsInFlight, correlationIdToCalls);
            }
            int numTimedOut = 0;
            TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
            synchronized (this) {
                numTimedOut += timeoutProcessor.handleTimeouts(newCalls,
                        "The AdminClient thread has exited.");
                newCalls = null;
            }
            numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                    "The AdminClient thread has exited.");
            if (numTimedOut > 0) {
                log.debug("Timed out {} remaining operations.", numTimedOut);
            }
            closeQuietly(client, "KafkaClient");
            closeQuietly(metrics, "Metrics");
            log.debug("Exiting AdminClientRunnable thread.");
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void enqueue(Call call, long now) {
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call, call.deadlineMs - now);
            }
            boolean accepted = false;
            synchronized (this) {
                if (newCalls != null) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            if (accepted) {
                client.wakeup(); // wake the thread if it is in poll()
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void call(Call call, long now) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("The AdminClient is not accepting new calls. Timing out {}.", call);
                call.fail(Long.MAX_VALUE, new TimeoutException("The AdminClient thread is not accepting new calls."));
            } else {
                enqueue(call, now);
            }
        }
    }

}
