package org.apache.rocketmq.grpc.benchmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.rocketmq.client.apis.ClientException;
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
        int threads = options.integer("threads", 64);
        int maxInflight = options.integer("max-inflight", 1024);
        byte[] body = new byte[options.integer("message-size", 128)];
        byte[] property = new byte[options.integer("property-size", 0)];
        java.util.Arrays.fill(body, (byte) 'a');
        java.util.Arrays.fill(property, (byte) 'p');
        String propertyValue = new String(property, java.nio.charset.StandardCharsets.US_ASCII);
        AtomicLong sequence = new AtomicLong();
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
             Metrics metrics = metrics(options, "producer-" + topicType + (async ? "-async" : "-sync"))) {
            System.out.printf("Producer started: topic=%s type=%s mode=%s threads=%d%n",
                topic, topicType, async ? "async" : "sync", threads);
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
                        try {
                            Message message = message(provider, options, topicType, topic, body, propertyValue, id);
                            if (async) {
                                producer.sendAsync(message).whenComplete((receipt, error) -> {
                                    long latency = System.nanoTime() - start;
                                    if (error == null) metrics.success(body.length, latency, null, null);
                                    else metrics.failure(latency);
                                    inflight.release();
                                });
                            } else if ("tx".equals(topicType)) {
                                Transaction transaction = producer.beginTransaction();
                                producer.send(message, transaction);
                                if ("ROLLBACK".equals(message.getProperties().get(TX_RESOLUTION_PROPERTY))) {
                                    transaction.rollback();
                                } else {
                                    transaction.commit();
                                }
                                metrics.success(body.length, System.nanoTime() - start, null, null);
                            } else {
                                producer.send(message);
                                metrics.success(body.length, System.nanoTime() - start, null, null);
                            }
                        } catch (Throwable t) {
                            metrics.failure(System.nanoTime() - start);
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
            metrics.reportFinal();
        } finally {
            lifecycle.stop();
            stopWorkers(workers);
        }
    }

    private static Message message(ClientServiceProvider provider, Options options, String topicType, String topic,
        byte[] body, String propertyValue, long id) {
        MessageBuilder builder = provider.newMessageBuilder().setTopic(topic).setBody(body);
        String tag = options.get("tag", "");
        if (!tag.isEmpty()) builder.setTag(tag);
        if (options.bool("key-enabled", false)) builder.setKeys("benchmark-" + id);
        if (!propertyValue.isEmpty()) builder.addProperty("benchmarkProperty", propertyValue);
        if ("fifo".equals(topicType)) {
            builder.setMessageGroup("benchmark-group-" + (id % options.integer("message-groups", 64)));
        } else if ("delay".equals(topicType)) {
            builder.setDeliveryTimestamp(System.currentTimeMillis() + options.longValue("delay-ms", 10000));
        } else if ("tx".equals(topicType)) {
            boolean commit = ThreadLocalRandom.current().nextInt(100) < options.integer("commit-percent", 100);
            builder.addProperty(TX_RESOLUTION_PROPERTY, commit ? "COMMIT" : "ROLLBACK");
        }
        return builder.build();
    }

    private static void runPushConsumer(Options options) throws Exception {
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        Lifecycle lifecycle = new Lifecycle();
        Pacer pacer = new Pacer(options.longValue("rate", 0));
        String topic = options.get("topic", null);
        int threads = options.integer("threads", 20);
        try (Metrics metrics = metrics(options, "push-consumer")) {
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
                    try {
                        pacer.acquire();
                        MessageTimes times = messageTimes(message);
                        metrics.success(bodySize(message), System.nanoTime() - start, times.e2eMillis, times.lagMillis);
                        return ConsumeResult.SUCCESS;
                    } catch (Throwable t) {
                        metrics.failure(System.nanoTime() - start);
                        return ConsumeResult.FAILURE;
                    }
                });
            try (PushConsumer ignored = builder.build()) {
                System.out.printf("PushConsumer started: topic=%s group=%s threads=%d%n",
                    topic, options.get("consumer-group", null), threads);
                runPhases(options, lifecycle, metrics);
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
        ExecutorService workers = Executors.newFixedThreadPool(threads, new NamedFactory("simple-consumer"));
        try (SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
                 .setClientConfiguration(clientConfiguration(options))
                 .setConsumerGroup(options.get("consumer-group", null))
                 .setAwaitDuration(Duration.ofMillis(options.longValue("await-ms", 30000)))
                 .setSubscriptionExpressions(Collections.singletonMap(topic, filter(options)))
                 .build();
             Metrics metrics = metrics(options, "simple-consumer")) {
            int batchSize = options.integer("batch-size", 16);
            Duration invisible = Duration.ofMillis(options.longValue("invisible-ms", 15000));
            System.out.printf("SimpleConsumer started: topic=%s group=%s threads=%d batch=%d%n",
                topic, options.get("consumer-group", null), threads, batchSize);
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
                                try {
                                    consumer.ack(message);
                                    MessageTimes times = messageTimes(message);
                                    metrics.success(bodySize(message), System.nanoTime() - start,
                                        times.e2eMillis, times.lagMillis);
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

    private static MessageTimes messageTimes(MessageView message) {
        long now = System.currentTimeMillis();
        Long e2e = now - message.getBornTimestamp();
        Long lag = message.getDeliveryTimestamp().isPresent() ? now - message.getDeliveryTimestamp().get() : null;
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
