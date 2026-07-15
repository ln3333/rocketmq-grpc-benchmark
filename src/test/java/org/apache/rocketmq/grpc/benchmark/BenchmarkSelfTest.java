package org.apache.rocketmq.grpc.benchmark;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BenchmarkSelfTest {
    private BenchmarkSelfTest() { }

    public static void main(String[] args) throws Exception {
        Options normal = Options.parse(new String[] {
            "producer", "--endpoints", "localhost:8081", "-t", "normal-topic"
        });
        assert normal.command == Options.Command.PRODUCER;
        assert normal.integer("threads", 64) == 64;
        assert "normal".equals(normal.get("topic-type", "normal"));

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
            metrics.success(128, 1_000_000, 2L, null);
            metrics.failure(2_000_000);
            metrics.reportFinal();
        }
        String csvText = new String(Files.readAllBytes(csv), StandardCharsets.UTF_8);
        assert csvText.startsWith("timestamp,phase,scope");
        assert csvText.contains(",final,self-test,");
        Files.deleteIfExists(csv);

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
}
