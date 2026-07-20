package org.apache.rocketmq.grpc.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.apache.rocketmq.client.apis.producer.TransactionResolution;

public final class BenchmarkMain {
    private static final String TX_RESOLUTION_PROPERTY = "benchmarkTxResolution";
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private BenchmarkMain() { }

    public static void main(String[] args) {
        try {
            Options options = Options.parse(args);
            if (options.bool("help", false)) {
                System.out.print(Options.help(options.command));
                return;
            }
            switch (options.command) {
                case PRODUCER: runProducer(options); break;
                case PUSH_CONSUMER: runPushConsumer(options); break;
                case SIMPLE_CONSUMER: runSimpleConsumer(options); break;
                default: throw new IllegalStateException("Unsupported command: " + options.command);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Run the selected command with --help for usage.");
            System.exit(2);
        } catch (Throwable t) {
            System.err.println("Benchmark failed: " + t);
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void runProducer(Options options) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Lifecycle lifecycle = new Lifecycle();
        String topic = options.get("topic", null);
        String topicType = options.get("topic-type", "normal");
        boolean async = "async".equals(options.get("send-mode", "sync"));
        boolean traceEnabled = options.bool("trace-enabled", true);
        int threads = options.integer("threads", 64);
        int maxInflight = options.integer("max-inflight", 1024);
        byte[] body = new byte[options.integer("message-size", 128)];
        byte[] property = new byte[options.integer("property-size", 0)];
        java.util.Arrays.fill(body, (byte) 'a');
        java.util.Arrays.fill(property, (byte) 'p');
        String propertyValue = new String(property, java.nio.charset.StandardCharsets.US_ASCII);
        AtomicLong sequence = new AtomicLong();
        String instanceId = MessageTrace.newInstanceId();
        Pacer pacer = new Pacer(options.longValue("rate", 0));
        Semaphore inflight = new Semaphore(maxInflight);

        ProducerBuilder builder = provider.newProducerBuilder()
            .setClientConfiguration(clientConfiguration(options))
            .setTopics(topic)
            .setMaxAttempts(options.integer("max-attempts", 3));
        if ("tx".equals(topicType)) {
            builder.setTransactionChecker(message -> "ROLLBACK".equals(
                message.getProperties().get(TX_RESOLUTION_PROPERTY))
                ? TransactionResolution.ROLLBACK : TransactionResolution.COMMIT);
        }

        ExecutorService workers = Executors.newFixedThreadPool(threads, new NamedFactory("producer"));
        try (Producer producer = builder.build();
             Metrics metrics = metrics(options, "producer-" + topicType + (async ? "-async" : "-sync"));
             MessageJournal journal = journal(options, "producer", instanceId)) {
            System.out.printf("Producer started: topic=%s type=%s mode=%s threads=%d trace=%s journal=%s%n",
                topic, topicType, async ? "async" : "sync", threads, traceEnabled,
                options.get("journal", null) == null ? "off" : options.get("journal", null));
            for (int i = 0; i < threads; i++) {
                workers.execute(() -> {
                    while (lifecycle.running()) {
                        pacer.acquire();
                        if (!lifecycle.running()) break;
                        if (async) {
                            try {
                                if (!inflight.tryAcquire(1, TimeUnit.SECONDS)) continue;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        long id = sequence.incrementAndGet();
                        long start = System.nanoTime();
                        BuiltMessage built = message(provider, options, topicType, topic, body, propertyValue,
                            id, instanceId, traceEnabled);
                        try {
                            if (async) {
                                producer.sendAsync(built.message).whenComplete((receipt, error) -> {
                                    long latency = System.nanoTime() - start;
                                    if (error == null) {
                                        metrics.success(body.length, latency, null, null);
                                        journalSent(journal, built, messageId(receipt), true);
                                    } else {
                                        metrics.failure(latency);
                                        journalSent(journal, built, null, false);
                                    }
                                    inflight.release();
                                });
                            } else if ("tx".equals(topicType)) {
                                Transaction transaction = producer.beginTransaction();
                                SendReceipt receipt = producer.send(built.message, transaction);
                                if ("ROLLBACK".equals(built.message.getProperties().get(TX_RESOLUTION_PROPERTY))) {
                                    transaction.rollback();
                                } else {
                                    transaction.commit();
                                }
                                metrics.success(body.length, System.nanoTime() - start, null, null);
                                journalSent(journal, built, messageId(receipt), true);
                            } else {
                                SendReceipt receipt = producer.send(built.message);
                                metrics.success(body.length, System.nanoTime() - start, null, null);
                                journalSent(journal, built, messageId(receipt), true);
                            }
                        } catch (Throwable t) {
                            metrics.failure(System.nanoTime() - start);
                            journalSent(journal, built, null, false);
                            if (async) inflight.release();
                        }
                    }
                });
            }
            runPhases(options, lifecycle, metrics);
            stopWorkers(workers);
            if (async && !inflight.tryAcquire(maxInflight, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                System.err.println("Timed out waiting for asynchronous sends to finish.");
            }
            if (journal != null) journal.flush();
            metrics.reportFinal();
        } finally {
            lifecycle.stop();
            stopWorkers(workers);
        }
    }

    private static BuiltMessage message(ClientServiceProvider provider, Options options, String topicType,
        String topic, byte[] body, String propertyValue, long id, String instanceId, boolean traceEnabled) {
        MessageBuilder builder = provider.newMessageBuilder().setTopic(topic).setBody(body);
        String tag = options.get("tag", "");
        if (!tag.isEmpty()) builder.setTag(tag);
        String key = null;
        long sendTs = -1L;
        if (traceEnabled) {
            key = MessageTrace.key(instanceId, id);
            sendTs = System.currentTimeMillis();
            builder.setKeys(key);
            builder.addProperty(MessageTrace.SEQ_PROPERTY, Long.toString(id));
            builder.addProperty(MessageTrace.SEND_TS_PROPERTY, Long.toString(sendTs));
        } else if (options.bool("key-enabled", false)) {
            key = MessageTrace.key(instanceId, id);
            builder.setKeys(key);
        }
        if (!propertyValue.isEmpty()) builder.addProperty("benchmarkProperty", propertyValue);
        if ("fifo".equals(topicType)) {
            builder.setMessageGroup("benchmark-group-" + (id % options.integer("message-groups", 64)));
        } else if ("delay".equals(topicType)) {
            builder.setDeliveryTimestamp(System.currentTimeMillis() + options.longValue("delay-ms", 10000));
        } else if ("tx".equals(topicType)) {
            boolean commit = ThreadLocalRandom.current().nextInt(100) < options.integer("commit-percent", 100);
            builder.addProperty(TX_RESOLUTION_PROPERTY, commit ? "COMMIT" : "ROLLBACK");
        }
        return new BuiltMessage(builder.build(), key, id, sendTs);
    }

    private static void runPushConsumer(Options options) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Lifecycle lifecycle = new Lifecycle();
        Pacer pacer = new Pacer(options.longValue("rate", 0));
        String topic = options.get("topic", null);
        int threads = options.integer("threads", 20);
        String instanceId = MessageTrace.newInstanceId();
        Set<String> seenKeys = ConcurrentHashMap.newKeySet();
        try (Metrics metrics = metrics(options, "push-consumer");
             MessageJournal journal = journal(options, "consumer", instanceId)) {
            PushConsumerBuilder builder = provider.newPushConsumerBuilder()
                .setClientConfiguration(clientConfiguration(options))
                .setConsumerGroup(options.get("consumer-group", null))
                .setSubscriptionExpressions(Collections.singletonMap(topic, filter(options)))
                .setConsumptionThreadCount(threads)
                .setMaxCacheMessageCount(options.integer("max-cache-messages", 4096))
                .setMaxCacheMessageSizeInBytes(options.integer("max-cache-bytes", 64 * 1024 * 1024))
                .setEnableFifoConsumeAccelerator(options.bool("fifo-accelerator", false))
                .setMessageListener(message -> {
                    long start = System.nanoTime();
                    long recvTs = System.currentTimeMillis();
                    try {
                        pacer.acquire();
                        MessageTrace.TraceInfo trace = MessageTrace.inspect(message, seenKeys);
                        if (journal != null) {
                            journal.received(trace.key, trace.seq, trace.sendTs, trace.messageId, recvTs);
                        }
                        MessageTimes times = messageTimes(message, recvTs);
                        long doneTs = System.currentTimeMillis();
                        if (journal != null) {
                            journal.done(trace.key, trace.seq, trace.sendTs, trace.messageId, doneTs, trace.duplicate);
                        }
                        metrics.success(bodySize(message), System.nanoTime() - start, times.e2eMillis,
                            times.lagMillis, trace.duplicate);
                        return ConsumeResult.SUCCESS;
                    } catch (Throwable t) {
                        metrics.failure(System.nanoTime() - start);
                        return ConsumeResult.FAILURE;
                    }
                });
            try (PushConsumer ignored = builder.build()) {
                System.out.printf("PushConsumer started: topic=%s group=%s threads=%d journal=%s%n",
                    topic, options.get("consumer-group", null), threads,
                    options.get("journal", null) == null ? "off" : options.get("journal", null));
                runPhases(options, lifecycle, metrics);
                if (journal != null) journal.flush();
                metrics.reportFinal();
            } finally {
                lifecycle.stop();
            }
        }
    }

    private static void runSimpleConsumer(Options options) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Lifecycle lifecycle = new Lifecycle();
        String topic = options.get("topic", null);
        int threads = options.integer("threads", 4);
        Pacer pacer = new Pacer(options.longValue("rate", 0));
        String instanceId = MessageTrace.newInstanceId();
        Set<String> seenKeys = ConcurrentHashMap.newKeySet();
        ExecutorService workers = Executors.newFixedThreadPool(threads, new NamedFactory("simple-consumer"));
        try (SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                 .setClientConfiguration(clientConfiguration(options))
                 .setConsumerGroup(options.get("consumer-group", null))
                 .setAwaitDuration(Duration.ofMillis(options.longValue("await-ms", 30000)))
                 .setSubscriptionExpressions(Collections.singletonMap(topic, filter(options)))
                 .build();
             Metrics metrics = metrics(options, "simple-consumer");
             MessageJournal journal = journal(options, "consumer", instanceId)) {
            int batchSize = options.integer("batch-size", 16);
            Duration invisible = Duration.ofMillis(options.longValue("invisible-ms", 15000));
            System.out.printf("SimpleConsumer started: topic=%s group=%s threads=%d batch=%d journal=%s%n",
                topic, options.get("consumer-group", null), threads, batchSize,
                options.get("journal", null) == null ? "off" : options.get("journal", null));
            for (int i = 0; i < threads; i++) {
                workers.execute(() -> {
                    while (lifecycle.running()) {
                        long receiveStart = System.nanoTime();
                        try {
                            List<MessageView> messages = consumer.receive(batchSize, invisible);
                            for (MessageView message : messages) {
                                if (!lifecycle.running()) break;
                                pacer.acquire();
                                long start = System.nanoTime();
                                long recvTs = System.currentTimeMillis();
                                MessageTrace.TraceInfo trace = MessageTrace.inspect(message, seenKeys);
                                if (journal != null) {
                                    journal.received(trace.key, trace.seq, trace.sendTs, trace.messageId, recvTs);
                                }
                                try {
                                    consumer.ack(message);
                                    long doneTs = System.currentTimeMillis();
                                    MessageTimes times = messageTimes(message, doneTs);
                                    if (journal != null) {
                                        journal.done(trace.key, trace.seq, trace.sendTs, trace.messageId, doneTs,
                                            trace.duplicate);
                                    }
                                    metrics.success(bodySize(message), System.nanoTime() - start,
                                        times.e2eMillis, times.lagMillis, trace.duplicate);
                                } catch (Throwable t) {
                                    metrics.failure(System.nanoTime() - start);
                                }
                            }
                        } catch (Throwable t) {
                            metrics.failure(System.nanoTime() - receiveStart);
                        }
                    }
                });
            }
            runPhases(options, lifecycle, metrics);
            stopWorkers(workers);
            if (journal != null) journal.flush();
            metrics.reportFinal();
        } finally {
            lifecycle.stop();
            stopWorkers(workers);
        }
    }

    private static ClientConfiguration clientConfiguration(Options options) {
        ClientConfigurationBuilder builder = ClientConfiguration.newBuilder()
            .setEndpoints(options.endpoints())
            .enableSsl(options.bool("ssl", true))
            .setRequestTimeout(Duration.ofMillis(options.longValue("request-timeout-ms", 3000)))
            .setMaxStartupAttempts(options.integer("max-startup-attempts", 3));
        String namespace = options.get("namespace", "");
        if (!namespace.isEmpty()) builder.setNamespace(namespace);
        if (options.accessKey() != null) {
            builder.setCredentialProvider(options.securityToken() == null
                ? new StaticSessionCredentialsProvider(options.accessKey(), options.secretKey())
                : new StaticSessionCredentialsProvider(options.accessKey(), options.secretKey(), options.securityToken()));
        }
        return builder.build();
    }

    private static FilterExpression filter(Options options) {
        FilterExpressionType type = "sql92".equals(options.get("filter-type", "tag"))
            ? FilterExpressionType.SQL92 : FilterExpressionType.TAG;
        return new FilterExpression(options.get("filter-expression", "*"), type);
    }

    private static Metrics metrics(Options options, String scope) throws IOException {
        return new Metrics(scope, options.integer("report-interval-seconds", 10), options.get("csv", null));
    }

    private static MessageJournal journal(Options options, String role, String instanceId) throws IOException {
        String path = options.get("journal", null);
        if (path == null || path.isEmpty()) return null;
        return new MessageJournal(path, role, instanceId);
    }

    private static void journalSent(MessageJournal journal, BuiltMessage built, String messageId, boolean ok) {
        if (journal == null || built.key == null) return;
        journal.sent(built.key, built.seq, built.sendTs, messageId, ok);
    }

    private static String messageId(SendReceipt receipt) {
        return receipt == null || receipt.getMessageId() == null ? null : receipt.getMessageId().toString();
    }

    private static void runPhases(Options options, Lifecycle lifecycle, Metrics metrics) throws InterruptedException {
        long warmup = options.longValue("warmup-seconds", 10);
        if (warmup > 0 && !lifecycle.await(warmup, TimeUnit.SECONDS)) return;
        if (!lifecycle.running()) return;
        metrics.beginMeasurement();
        long duration = options.longValue("duration-seconds", 0);
        if (duration == 0) lifecycle.await();
        else lifecycle.await(duration, TimeUnit.SECONDS);
        lifecycle.stop();
    }

    private static int bodySize(MessageView message) {
        ByteBuffer body = message.getBody();
        return body.remaining();
    }

    private static MessageTimes messageTimes(MessageView message, long nowMillis) {
        Long e2e = MessageTrace.e2eMillis(message, nowMillis);
        Long lag = message.getDeliveryTimestamp().isPresent()
            ? nowMillis - message.getDeliveryTimestamp().get() : null;
        return new MessageTimes(e2e, lag);
    }

    private static void stopWorkers(ExecutorService workers) {
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                System.err.println("Timed out waiting for benchmark workers to stop.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class BuiltMessage {
        final Message message;
        final String key;
        final long seq;
        final long sendTs;

        BuiltMessage(Message message, String key, long seq, long sendTs) {
            this.message = message;
            this.key = key;
            this.seq = seq;
            this.sendTs = sendTs;
        }
    }

    private static final class MessageTimes {
        final Long e2eMillis;
        final Long lagMillis;
        MessageTimes(Long e2eMillis, Long lagMillis) { this.e2eMillis = e2eMillis; this.lagMillis = lagMillis; }
    }

    private static final class Lifecycle {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final CountDownLatch stopped = new CountDownLatch(1);

        Lifecycle() { Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "benchmark-shutdown")); }
        boolean running() { return running.get(); }
        void stop() { if (running.compareAndSet(true, false)) stopped.countDown(); }
        void await() throws InterruptedException { stopped.await(); }
        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return !stopped.await(timeout, unit);
        }
    }

    private static final class Pacer {
        private final long intervalNanos;
        private final AtomicLong next = new AtomicLong();

        Pacer(long rate) { intervalNanos = rate <= 0 ? 0 : Math.max(1, 1_000_000_000L / rate); }
        void acquire() {
            if (intervalNanos == 0) return;
            while (true) {
                long now = System.nanoTime();
                long previous = next.get();
                long slot = Math.max(now, previous);
                if (next.compareAndSet(previous, slot + intervalNanos)) {
                    long wait = slot - now;
                    if (wait > 0) LockSupport.parkNanos(wait);
                    return;
                }
            }
        }
    }

    private static final class NamedFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicLong number = new AtomicLong();
        NamedFactory(String prefix) { this.prefix = prefix; }
        @Override public Thread newThread(Runnable runnable) {
            return new Thread(runnable, prefix + "-" + number.incrementAndGet());
        }
    }
}
