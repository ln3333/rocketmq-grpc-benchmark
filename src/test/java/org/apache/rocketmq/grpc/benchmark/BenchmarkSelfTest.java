package org.apache.rocketmq.grpc.benchmark;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class BenchmarkSelfTest {
    private BenchmarkSelfTest() { }

    public static void main(String[] args) throws Exception {
        Options normal = Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "-t", "normal-topic"
        });
        assert normal.command == Options.Command.PRODUCER;
        assert normal.integer("threads", 64) == 64;
        assert "normal".equals(normal.get("topic-type", "normal"));
        assert normal.bool("trace-enabled", true);

        Options tx0 = Options.parse(new String[] {
            "producer", "--endpoints=localhost:8081", "--topic", "tx-topic",
            "--topic-type", "tx", "--commit-percent", "0"
        });
        assert tx0.integer("commit-percent", 100) == 0;
        Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "--topic", "tx-topic",
            "--topic-type", "tx", "--commit-percent", "100"
        });
        expectFailure("transaction async", () -> Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "--topic", "tx-topic",
            "--topic-type", "tx", "--send-mode", "async"
        }));
        expectFailure("commit percentage", () -> Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "--topic", "tx-topic",
            "--topic-type", "tx", "--commit-percent", "101"
        }));
        expectFailure("partial credentials", () -> Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "--topic", "topic",
            "--access-key", "ak"
        }));

        Options traced = Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "-t", "topic",
            "--trace-enabled", "false", "--journal", "/tmp/demo.journal"
        });
        assert !traced.bool("trace-enabled", true);
        assert "/tmp/demo.journal".equals(traced.get("journal", null));

        Options consumer = Options.parse(new String[] {
            "push-consumer", "--endpoints", "localhost:8081", "-t", "topic",
            "-g", "cg", "--journal", "/tmp/consume.journal"
        });
        assert consumer.bool("trace-enabled", true);
        assert "/tmp/consume.journal".equals(consumer.get("journal", null));

        Metrics.Histogram histogram = new Metrics.Histogram();
        histogram.recordMillis(1);
        histogram.recordMillis(2);
        histogram.recordMillis(100);
        Metrics.HistogramSnapshot snapshot = histogram.snapshot(false);
        assert snapshot.count == 3;
        assert snapshot.percentileMicros(50) >= 2_000;
        assert snapshot.percentileMicros(99) >= 100_000;

        Path csv = Files.createTempFile("rocketmq-benchmark-", ".csv");
        try (Metrics metrics = new Metrics("self-test", 60, csv.toString())) {
            metrics.beginMeasurement();
            metrics.success(128, 1_000_000, 2L, null, false);
            metrics.success(128, 1_000_000, 2L, null, true);
            metrics.failure(2_000_000);
            metrics.reportFinal();
        }
        String csvText = new String(Files.readAllBytes(csv), StandardCharsets.UTF_8);
        assert csvText.startsWith("timestamp,phase,scope");
        assert csvText.contains("duplicates");
        assert csvText.contains(",final,self-test,");
        Files.deleteIfExists(csv);

        String instanceId = MessageTrace.newInstanceId();
        assert instanceId.length() == 32;
        assert MessageTrace.key(instanceId, 7).equals("benchmark-" + instanceId + "-7");

        Path journalPath = Files.createTempFile("rocketmq-benchmark-journal-", ".log");
        try (MessageJournal journal = new MessageJournal(journalPath.toString(), "producer", instanceId)) {
            journal.sent("benchmark-" + instanceId + "-1", 1, 1000L, "mid-1", true);
            journal.sent("benchmark-" + instanceId + "-2", 2, 1001L, null, false);
            journal.flush();
        }
        String journalText = new String(Files.readAllBytes(journalPath), StandardCharsets.UTF_8);
        assert journalText.contains("role=producer");
        assert journalText.contains(",sent,");
        assert journalText.contains(",ok,");
        assert journalText.contains(",fail,");
        Files.deleteIfExists(journalPath);

        Path consumerJournal = Files.createTempFile("rocketmq-benchmark-cjournal-", ".log");
        try (MessageJournal journal = new MessageJournal(consumerJournal.toString(), "consumer", instanceId)) {
            journal.received("k1", 1, 1000L, "mid-1", 1100L);
            journal.done("k1", 1, 1000L, "mid-1", 1200L, false);
            journal.done("k1", 1, 1000L, "mid-1", 1300L, true);
            journal.flush();
        }
        String consumerText = new String(Files.readAllBytes(consumerJournal), StandardCharsets.UTF_8);
        assert consumerText.contains(",recv,");
        assert consumerText.contains(",done,");
        assert consumerText.contains(",unique");
        assert consumerText.contains(",dup");
        Files.deleteIfExists(consumerJournal);

        Set<String> seen = ConcurrentHashMap.newKeySet();
        FakeView first = new FakeView("mid-a", Collections.singleton("k-a"), "1", "100");
        FakeView again = new FakeView("mid-b", Collections.singleton("k-a"), "1", "100");
        MessageTrace.TraceInfo one = MessageTrace.inspect(first, seen);
        MessageTrace.TraceInfo two = MessageTrace.inspect(again, seen);
        assert !one.duplicate;
        assert two.duplicate;
        assert MessageTrace.e2eMillis(first, 150L) == 50L;

        System.out.println("BenchmarkSelfTest passed");
    }

    private static void expectFailure(String name, Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected failure: " + name);
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    /** Minimal MessageView stand-in for MessageTrace unit checks. */
    private static final class FakeView implements org.apache.rocketmq.client.apis.message.MessageView {
        private final String messageId;
        private final java.util.Collection<String> keys;
        private final java.util.Map<String, String> properties;

        FakeView(String messageId, java.util.Collection<String> keys, String seq, String sendTs) {
            this.messageId = messageId;
            this.keys = keys;
            this.properties = new java.util.HashMap<>();
            properties.put(MessageTrace.SEQ_PROPERTY, seq);
            properties.put(MessageTrace.SEND_TS_PROPERTY, sendTs);
        }

        @Override public org.apache.rocketmq.client.apis.message.MessageId getMessageId() {
            return new org.apache.rocketmq.client.apis.message.MessageId() {
                @Override public String getVersion() { return "v1"; }
                @Override public String toString() { return messageId; }
            };
        }
        @Override public String getTopic() { return "t"; }
        @Override public java.nio.ByteBuffer getBody() { return java.nio.ByteBuffer.wrap(new byte[0]); }
        @Override public java.util.Map<String, String> getProperties() { return properties; }
        @Override public java.util.Optional<String> getTag() { return java.util.Optional.empty(); }
        @Override public java.util.Collection<String> getKeys() { return keys; }
        @Override public java.util.Optional<String> getMessageGroup() { return java.util.Optional.empty(); }
        @Override public java.util.Optional<String> getLiteTopic() { return java.util.Optional.empty(); }
        @Override public java.util.Optional<Long> getDeliveryTimestamp() { return java.util.Optional.empty(); }
        @Override public java.util.Optional<Integer> getPriority() { return java.util.Optional.empty(); }
        @Override public String getBornHost() { return "localhost"; }
        @Override public long getBornTimestamp() { return 0L; }
        @Override public int getDeliveryAttempt() { return 1; }
    }
}
