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

package de.ux.kafka.clients.admin.request;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;
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

    /**
     * Thread name prefix for admin client network thread
     */
    static final String NETWORK_THREAD_PREFIX = "kafka-admin-client-thread";

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
     * The cluster metadata manager used by the KafkaClient.
     */
    private final AdminMetadataManager metadataManager;

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
    private final RequestClientRunnable runnable;

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

    private final long retryBackoffMs;

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
     * Create a new RequestClient with the given configuration.
     *
     * @param props The configuration.
     * @return The new RequestClient.
     */
    public static RequestClient create(Properties props) {
        return createInternal(new AdminClientConfig(props), null);
    }

    /**
     * Create a new RequestClient with the given configuration.
     *
     * @param conf The configuration.
     * @return The new RequestClient.
     */
    public static RequestClient create(Map<String, ?> conf) {
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
            AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
                    config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                    config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG));
            metadataManager.update(Cluster.bootstrap(addresses), time.milliseconds());
            List<MetricsReporter> reporters = config.getConfiguredInstances(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class,
                    Collections.singletonMap(AdminClientConfig.CLIENT_ID_CONFIG, clientId));
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            reporters.add(new JmxReporter(JMX_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time);
            String metricGrpPrefix = "admin-client";
            channelBuilder = ClientUtils.createChannelBuilder(config, time);
            selector = new Selector(config.getLong(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                    metrics, time, metricGrpPrefix, channelBuilder, logContext);
            networkClient = new NetworkClient(
                    selector,
                    metadataManager.updater(),
                    clientId,
                    1,
                    config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getLong(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                    config.getInt(AdminClientConfig.SEND_BUFFER_CONFIG),
                    config.getInt(AdminClientConfig.RECEIVE_BUFFER_CONFIG),
                    (int) TimeUnit.HOURS.toMillis(1),
                    ClientDnsLookup.forConfig(config.getString(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                    time,
                    true,
                    apiVersions,
                    logContext);
            return new RequestClient(config, clientId, time, metadataManager, metrics, networkClient,
                    timeoutProcessorFactory, logContext);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            closeQuietly(selector, "Selector");
            closeQuietly(channelBuilder, "ChannelBuilder");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    static LogContext createLogContext(String clientId) {
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    private RequestClient(AdminClientConfig config,
                             String clientId,
                             Time time,
                             AdminMetadataManager metadataManager,
                             Metrics metrics,
                             KafkaClient client,
                             TimeoutProcessorFactory timeoutProcessorFactory,
                             LogContext logContext) {
        this.defaultTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.time = time;
        this.metadataManager = metadataManager;
        this.metrics = metrics;
        this.client = client;
        this.runnable = new RequestClientRunnable();
        String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
                new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, Time.SYSTEM.milliseconds());
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    Time time() {
        return time;
    }

    public NodeProvider toNode(int nodeId) {
        return new ConstantNodeIdProvider(nodeId);
    }

    public NodeProvider toControllerNode() {
        return new ControllerNodeProvider();
    }

    public NodeProvider toLeastLoadedNode() {
        return new LeastLoadedNodeProvider();
    }

    public List<NodeProvider> toAllNodes() {
        if(!metadataManager.isReady()) {
            metadataManager.requestUpdate();
        }
        try {
            while(!metadataManager.isReady()) {
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            //Do nothing
        }
        return metadataManager.updater().fetchNodes().stream()
                .map(Node::id)
                .map(ConstantNodeIdProvider::new)
                .collect(toList());
    }

    public <T extends AbstractResponse> List<ResponseResult<T>> request(final List<RequestDefinition<T, ?>> requestDefinitions, final NodeProvider nodeProvider) {
        return requestDefinitions.stream()
                .map(requestDefinition -> request(requestDefinition, nodeProvider))
                .collect(toList());
    }

    public <T extends AbstractResponse> List<ResponseResult<T>> request(final RequestDefinition<T, ?> requestDefinition, final List<NodeProvider> nodeProviders) {
        return nodeProviders.stream()
                .map(nodeProvider -> request(requestDefinition, nodeProvider))
                .collect(toList());
    }

    public <T extends AbstractResponse> ResponseResult<T> request(final RequestDefinition<T, ?> requestDefinition, final NodeProvider nodeProvider) {
        final KafkaFutureImpl<ImmutablePair<Node, T>> responseFuture = new KafkaFutureImpl<>();
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
     * Close the RequestClient and release all associated resources.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    public void close(Duration timeout) {
        long waitTimeMs = timeout.toMillis();
        if (waitTimeMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
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

    private class MetadataUpdateNodeIdProvider implements NodeProvider {
        @Override
        public Node provide() {
            return client.leastLoadedNode(time.milliseconds());
        }

        @Override
        public String toString() {
            return "Least Loaded Node";
        }
    }


    private class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId;

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.nodeById(nodeId) != null)) {
                return metadataManager.nodeById(nodeId);
            }
            // If we can't find the node with the given constant ID, we schedule a
            // metadata update and hope it appears.  This behavior is useful for avoiding
            // flaky behavior in tests when the cluster is starting up and not all nodes
            // have appeared.
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public String toString() {
            return "Node " + nodeId;
        }
    }

    /**
     * Provides the controller node.
     */
    private class ControllerNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.controller() != null)) {
                return metadataManager.controller();
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public String toString() {
            return "Controller Node";
        }
    }

    /**
     * Provides the least loaded node.
     */
    private class LeastLoadedNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady()) {
                // This may return null if all nodes are busy.
                // In that case, we will postpone node assignment.
                return client.leastLoadedNode(time.milliseconds());
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public String toString() {
            return "Least Loaded Node";
        }
    }

    class RequestResponseCall<T extends AbstractResponse> extends Call {

        private final RequestDefinition<T, ?> requestDefinition;
        private final KafkaFutureImpl<ImmutablePair<Node, T>> responseFuture;

        RequestResponseCall(RequestDefinition<T, ?> requestDefinition, NodeProvider nodeProvider, long deadlineMs, KafkaFutureImpl<ImmutablePair<Node, T>> responseFuture) {
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
            responseFuture.complete(ImmutablePair.of(curNode(), requestDefinition.response(abstractResponse)));
        }

        @Override
        void handleFailure(Throwable throwable) {
            responseFuture.completeExceptionally(throwable);
        }
    }

    abstract class Call {
        private final boolean internal;
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        private int tries = 0;
        private boolean aborted = false;
        private Node curNode = null;
        private long nextAllowedTryMs = 0;

        Call(boolean internal, String callName, long deadlineMs, NodeProvider nodeProvider) {
            this.internal = internal;
            this.callName = callName;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, deadlineMs, nodeProvider);
        }

        protected Node curNode() {
            return curNode;
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
                log.debug("{} attempting protocol downgrade and then retry.", this);
                runnable.enqueue(this, now);
                return;
            }
            tries++;
            nextAllowedTryMs = now + retryBackoffMs;

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

        public boolean isInternal() {
            return internal;
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
        int handleTimeoutsForSent(Collection<Pair<Node, Call>> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Pair<Node, Call>> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next().getRight();
                if(handleTimeout(call, msg)) {
                    iter.remove();
                    numTimedOut++;
                }
            }
            return numTimedOut;
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
                if(handleTimeout(call, msg)) {
                    iter.remove();
                    numTimedOut++;
                }
            }
            return numTimedOut;
        }

        boolean handleTimeout(Call call, String msg) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0) {
                call.fail(now, new TimeoutException(msg));
                return true;
            } else {
                nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                return false;
            }
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

    private final class RequestClientRunnable implements Runnable {
        /**
         * Calls which have not yet been assigned to a node.
         * Only accessed from this thread.
         */
        private final ArrayList<Call> pendingCalls = new ArrayList<>();

        /**
         * Maps nodes to calls that we want to send.
         * Only accessed from this thread.
         */
        private final Map<Node, List<Call>> callsToSend = new HashMap<>();

        /**
         * Maps node ID strings to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<String, List<Call>> callsInFlight = new HashMap<>();

        /**
         * Maps correlation IDs to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

        /**
         * Pending calls. Protected by the object monitor.
         * This will be null only if the thread has shut down.
         */
        private List<Call> newCalls = new LinkedList<>();

        /**
         * Time out the elements in the pendingCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private void timeoutPendingCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} pending calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         */
        private int timeoutCallsToSend(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                        "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            return numTimedOut;
        }

        /**
         * Drain all the calls from newCalls into pendingCalls.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         */
        private synchronized void drainNewCalls() {
            if (!newCalls.isEmpty()) {
                pendingCalls.addAll(newCalls);
                newCalls.clear();
            }
        }

        /**
         * Choose nodes for the calls in the pendingCalls list.
         *
         * @param now           The current time in milliseconds.
         * @return              The minimum time until a call is ready to be retried if any of the pending
         *                      calls are backing off after a failure
         */
        private long maybeDrainPendingCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

            Iterator<Call> pendingIter = pendingCalls.iterator();
            while (pendingIter.hasNext()) {
                Call call = pendingIter.next();

                // If the call is being retried, await the proper backoff before finding the node
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
                } else if (maybeDrainPendingCall(call, now)) {
                    pendingIter.remove();
                }
            }
            return pollTimeout;
        }

        /**
         * Check whether a pending call can be assigned a node. Return true if the pending call was either
         * transferred to the callsToSend collection or if the call was failed. Return false if it
         * should remain pending.
         */
        private boolean maybeDrainPendingCall(Call call, long now) {
            try {
                Node node = call.nodeProvider.provide();
                if (node != null) {
                    log.trace("Assigned {} to node {}", call, node);
                    call.curNode = node;
                    getOrCreateListValue(callsToSend, node).add(call);
                    return true;
                } else {
                    log.trace("Unable to assign {} to a node.", call);
                    return false;
                }
            } catch (Throwable t) {
                // Handle authentication errors while choosing nodes.
                log.debug("Unable to choose node for {}", call, t);
                call.fail(now, t);
                return true;
            }
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (!client.ready(node, now)) {
                    long nodeTimeout = client.pollDelayMs(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                Call call = calls.remove(0);
                int timeoutMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                AbstractRequest.Builder<?> requestBuilder;
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
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor) {
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
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         **/
        private void handleResponses(long now, List<ClientResponse> responses) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCalls.get(correlationId);
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
                correlationIdToCalls.remove(correlationId);
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
                    AuthenticationException authException = client.authenticationException(call.curNode());
                    if (authException != null) {
                        call.fail(now, authException);
                    } else {
                        call.fail(now, new DisconnectException(String.format(
                                "Cancelled %s request with correlation id %s due to node %s being disconnected",
                                call.callName, correlationId, response.destination())));
                    }
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

        /**
         * Unassign calls that have not yet been sent based on some predicate. For example, this
         * is used to reassign the calls that have been assigned to a disconnected node.
         *
         * @param shouldUnassign Condition for reassignment. If the predicate is true, then the calls will
         *                       be put back in the pendingCalls collection and they will be reassigned
         */
        private void unassignUnsentCalls(Predicate<Node> shouldUnassign) {
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                Node node = entry.getKey();
                List<Call> awaitingCalls = entry.getValue();

                if (awaitingCalls.isEmpty()) {
                    iter.remove();
                } else if (shouldUnassign.test(node)) {
                    pendingCalls.addAll(awaitingCalls);
                    iter.remove();
                }
            }
        }

        private boolean hasActiveExternalCalls(Collection<Call> calls) {
            for (Call call : calls) {
                if (!call.isInternal()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Return true if there are currently active external calls.
         */
        private boolean hasActiveExternalCalls() {
            if (hasActiveExternalCalls(pendingCalls)) {
                return true;
            }
            for (List<Call> callList : callsToSend.values()) {
                if (hasActiveExternalCalls(callList)) {
                    return true;
                }
            }
            return hasActiveExternalCalls(correlationIdToCalls.values());
        }

        private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
            if (!hasActiveExternalCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            long now = time.milliseconds();
            log.trace("Thread starting");
            while (true) {
                // Copy newCalls into pendingCalls.
                drainNewCalls();

                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutPendingCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor);
                timeoutCallsInFlight(timeoutProcessor);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Choose nodes for our pending calls.
                pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
                long metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now);
                if (metadataFetchDelayMs == 0) {
                    metadataManager.transitionToUpdatePending(now);
                    Call metadataCall = makeMetadataCall(now);
                    // Create a new metadata fetch call and add it to the end of pendingCalls.
                    // Assign a node for just the new call (we handled the other pending nodes above).

                    if (!maybeDrainPendingCall(metadataCall, now))
                        pendingCalls.add(metadataCall);
                }
                pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

                if (metadataFetchDelayMs > 0) {
                    pollTimeout = Math.min(pollTimeout, metadataFetchDelayMs);
                }

                // Ensure that we use a small poll timeout if there are pending calls which need to be sent
                if (!pendingCalls.isEmpty())
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(pollTimeout, now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // unassign calls to disconnected nodes
                unassignUnsentCalls(client::connectionFailed);

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleResponses(now, responses);
            }
            int numTimedOut = 0;
            TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
            synchronized (this) {
                numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
                newCalls = null;
            }
            numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
            numTimedOut += timeoutCallsToSend(timeoutProcessor);
            numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                    "The AdminClient thread has exited.");
            if (numTimedOut > 0) {
                log.debug("Timed out {} remaining operation(s).", numTimedOut);
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

        /**
         * Create a new metadata call.
         */
        private Call makeMetadataCall(long now) {
            return new Call(true, "fetchMetadata", calcDeadlineMs(now, defaultTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public AbstractRequest.Builder createRequest(int timeoutMs) {
                    // Since this only requests node information, it's safe to pass true
                    // for allowAutoTopicCreation (and it simplifies communication with
                    // older brokers)
                    return new MetadataRequest.Builder(Collections.emptyList(), true);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    long now = time.milliseconds();
                    metadataManager.update(response.cluster(), now);

                    // Unassign all unsent requests after a metadata refresh to allow for a new
                    // destination to be selected from the new metadata
                    unassignUnsentCalls(node -> true);
                }

                @Override
                public void handleFailure(Throwable e) {
                    metadataManager.updateFailed(e);
                }
            };
        }
    }

}