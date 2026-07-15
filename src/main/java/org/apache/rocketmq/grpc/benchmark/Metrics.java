package org.apache.rocketmq.grpc.benchmark;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

final class Metrics implements Closeable {
    private final String scope;
    private final Counters interval = new Counters();
    private final Counters total = new Counters();
    private final ScheduledExecutorService reporter;
    private final BufferedWriter csv;
    private final long startedNanos = System.nanoTime();
    private volatile long measuredNanos = startedNanos;
    private volatile boolean measuring;

    Metrics(String scope, int reportIntervalSeconds, String csvPath) throws IOException {
        this.scope = scope;
        this.reporter = Executors.newSingleThreadScheduledExecutor(new NamedFactory("benchmark-reporter"));
        if (csvPath == null || csvPath.isEmpty()) {
            this.csv = null;
        } else {
            Path path = Paths.get(csvPath);
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            this.csv = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            csv.write("timestamp,phase,scope,elapsed_seconds,attempts,success,failures,tps,mbps,"
                + "lat_avg_ms,lat_p50_ms,lat_p95_ms,lat_p99_ms,lat_max_ms,"
                + "e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,delivery_lag_p50_ms,delivery_lag_p95_ms,"
                + "delivery_lag_p99_ms,clock_skew_samples\n");
            csv.flush();
        }
        reporter.scheduleAtFixedRate(() -> {
            try { reportInterval(); }
            catch (Throwable t) { System.err.println("Metrics reporter failed: " + t); }
        }, reportIntervalSeconds, reportIntervalSeconds, TimeUnit.SECONDS);
    }

    synchronized void beginMeasurement() {
        interval.snapshotAndReset();
        total.reset();
        measuredNanos = System.nanoTime();
        measuring = true;
        System.out.println("Warmup complete; measured benchmark started.");
    }

    void success(int bytes, long operationNanos, Long e2eMillis, Long deliveryLagMillis) {
        interval.record(true, bytes, operationNanos, e2eMillis, deliveryLagMillis);
        if (measuring) total.record(true, bytes, operationNanos, e2eMillis, deliveryLagMillis);
    }

    void failure(long operationNanos) {
        interval.record(false, 0, operationNanos, null, null);
        if (measuring) total.record(false, 0, operationNanos, null, null);
    }

    private synchronized void reportInterval() throws IOException {
        Snapshot snapshot = interval.snapshotAndReset();
        String phase = measuring ? "measure" : "warmup";
        double elapsed = snapshot.elapsedNanos / 1_000_000_000d;
        print(phase, snapshot, elapsed);
        writeCsv(phase, snapshot, elapsed);
    }

    synchronized void reportFinal() {
        try {
            Snapshot last = interval.snapshotAndReset();
            if (last.attempts > 0) {
                double elapsed = last.elapsedNanos / 1_000_000_000d;
                print(measuring ? "measure" : "warmup", last, elapsed);
                writeCsv(measuring ? "measure" : "warmup", last, elapsed);
            }
            Snapshot snapshot = total.snapshot();
            double elapsed = Math.max(1, System.nanoTime() - measuredNanos) / 1_000_000_000d;
            print("final", snapshot, elapsed);
            writeCsv("final", snapshot, elapsed);
            if (csv != null) csv.flush();
        } catch (IOException e) {
            System.err.println("Failed to write final metrics: " + e);
        }
    }

    private void print(String phase, Snapshot s, double seconds) {
        double tps = seconds == 0 ? 0 : s.success / seconds;
        double mbps = seconds == 0 ? 0 : s.bytes / 1024d / 1024d / seconds;
        StringBuilder line = new StringBuilder(String.format(Locale.ROOT,
            "%s %-7s %-15s attempts=%d success=%d fail=%d tps=%.2f MB/s=%.2f "
                + "lat(ms)=avg:%.3f,p50:%.3f,p95:%.3f,p99:%.3f,max:%.3f",
            Instant.now(), phase, scope, s.attempts, s.success, s.failures, tps, mbps,
            millis(s.latency.averageMicros()), millis(s.latency.percentileMicros(50)),
            millis(s.latency.percentileMicros(95)), millis(s.latency.percentileMicros(99)),
            s.latency.maxNanos / 1_000_000d));
        if (s.e2e.count > 0) {
            line.append(String.format(Locale.ROOT, " e2e(ms)=p50:%.3f,p95:%.3f,p99:%.3f",
                millis(s.e2e.percentileMicros(50)), millis(s.e2e.percentileMicros(95)),
                millis(s.e2e.percentileMicros(99))));
        }
        if (s.deliveryLag.count > 0) {
            line.append(String.format(Locale.ROOT, " deliveryLag(ms)=p50:%.3f,p95:%.3f,p99:%.3f",
                millis(s.deliveryLag.percentileMicros(50)), millis(s.deliveryLag.percentileMicros(95)),
                millis(s.deliveryLag.percentileMicros(99))));
        }
        if (s.clockSkew > 0) line.append(" clockSkewSamples=").append(s.clockSkew);
        System.out.println(line);
    }

    private void writeCsv(String phase, Snapshot s, double seconds) throws IOException {
        if (csv == null) return;
        double tps = seconds == 0 ? 0 : s.success / seconds;
        double mbps = seconds == 0 ? 0 : s.bytes / 1024d / 1024d / seconds;
        csv.write(String.format(Locale.ROOT,
            "%s,%s,%s,%.3f,%d,%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,"
                + "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d%n",
            Instant.now(), phase, scope, seconds, s.attempts, s.success, s.failures, tps, mbps,
            millis(s.latency.averageMicros()), millis(s.latency.percentileMicros(50)),
            millis(s.latency.percentileMicros(95)), millis(s.latency.percentileMicros(99)),
            s.latency.maxNanos / 1_000_000d,
            millis(s.e2e.percentileMicros(50)), millis(s.e2e.percentileMicros(95)),
            millis(s.e2e.percentileMicros(99)), millis(s.deliveryLag.percentileMicros(50)),
            millis(s.deliveryLag.percentileMicros(95)), millis(s.deliveryLag.percentileMicros(99)), s.clockSkew));
        csv.flush();
    }

    private static double millis(long micros) { return micros / 1000d; }

    @Override
    public synchronized void close() throws IOException {
        reporter.shutdownNow();
        if (csv != null) csv.close();
    }

    static final class Histogram {
        // 0-100ms: 0.1ms buckets; 100ms-1s: 1ms; 1s-10s: 10ms; 10s-60s: 100ms; overflow.
        private static final int BUCKETS = 3301;
        private final AtomicLongArray counts = new AtomicLongArray(BUCKETS);
        private final LongAdder count = new LongAdder();
        private final LongAdder sumMicros = new LongAdder();
        private final AtomicLong maxNanos = new AtomicLong();

        void recordNanos(long nanos) {
            long safe = Math.max(0, nanos);
            recordMicros(safe / 1000, safe);
        }

        void recordMillis(long millis) {
            long safe = Math.max(0, millis);
            recordMicros(safe * 1000, safe * 1_000_000);
        }

        private void recordMicros(long micros, long nanos) {
            counts.incrementAndGet(index(micros));
            count.increment();
            sumMicros.add(micros);
            maxNanos.accumulateAndGet(nanos, Math::max);
        }

        HistogramSnapshot snapshot(boolean reset) {
            long[] values = new long[BUCKETS];
            for (int i = 0; i < BUCKETS; i++) values[i] = reset ? counts.getAndSet(i, 0) : counts.get(i);
            long c = reset ? count.sumThenReset() : count.sum();
            long sum = reset ? sumMicros.sumThenReset() : sumMicros.sum();
            long max = reset ? maxNanos.getAndSet(0) : maxNanos.get();
            return new HistogramSnapshot(values, c, sum, max);
        }

        void reset() { snapshot(true); }

        private static int index(long micros) {
            if (micros < 100_000) return (int) (micros / 100);
            if (micros < 1_000_000) return 1000 + (int) ((micros - 100_000) / 1000);
            if (micros < 10_000_000) return 1900 + (int) ((micros - 1_000_000) / 10_000);
            if (micros < 60_000_000) return 2800 + (int) ((micros - 10_000_000) / 100_000);
            return 3300;
        }

        private static long upperMicros(int index) {
            if (index < 1000) return (index + 1L) * 100;
            if (index < 1900) return 100_000 + (index - 999L) * 1000;
            if (index < 2800) return 1_000_000 + (index - 1899L) * 10_000;
            if (index < 3300) return 10_000_000 + (index - 2799L) * 100_000;
            return 60_000_000;
        }
    }

    static final class HistogramSnapshot {
        final long[] counts;
        final long count;
        final long sumMicros;
        final long maxNanos;

        HistogramSnapshot(long[] counts, long count, long sumMicros, long maxNanos) {
            this.counts = counts;
            this.count = count;
            this.sumMicros = sumMicros;
            this.maxNanos = maxNanos;
        }

        long averageMicros() { return count == 0 ? 0 : sumMicros / count; }
        long percentileMicros(int percentile) {
            if (count == 0) return 0;
            long target = Math.max(1, (count * percentile + 99) / 100);
            long seen = 0;
            for (int i = 0; i < counts.length; i++) {
                seen += counts[i];
                if (seen >= target) return Histogram.upperMicros(i);
            }
            return Histogram.upperMicros(counts.length - 1);
        }
    }

    private static final class Counters {
        private final LongAdder attempts = new LongAdder();
        private final LongAdder success = new LongAdder();
        private final LongAdder failures = new LongAdder();
        private final LongAdder bytes = new LongAdder();
        private final LongAdder clockSkew = new LongAdder();
        private final Histogram latency = new Histogram();
        private final Histogram e2e = new Histogram();
        private final Histogram deliveryLag = new Histogram();
        private final AtomicLong lastSnapshot = new AtomicLong(System.nanoTime());

        void record(boolean succeeded, int byteCount, long operationNanos, Long e2eMillis, Long lagMillis) {
            attempts.increment();
            if (succeeded) success.increment(); else failures.increment();
            bytes.add(Math.max(0, byteCount));
            latency.recordNanos(operationNanos);
            if (e2eMillis != null) {
                if (e2eMillis >= 0) e2e.recordMillis(e2eMillis); else clockSkew.increment();
            }
            if (lagMillis != null) {
                if (lagMillis >= 0) deliveryLag.recordMillis(lagMillis); else clockSkew.increment();
            }
        }

        Snapshot snapshotAndReset() {
            long now = System.nanoTime();
            return new Snapshot(attempts.sumThenReset(), success.sumThenReset(), failures.sumThenReset(),
                bytes.sumThenReset(), clockSkew.sumThenReset(), latency.snapshot(true), e2e.snapshot(true),
                deliveryLag.snapshot(true), now - lastSnapshot.getAndSet(now));
        }

        Snapshot snapshot() {
            return new Snapshot(attempts.sum(), success.sum(), failures.sum(), bytes.sum(), clockSkew.sum(),
                latency.snapshot(false), e2e.snapshot(false), deliveryLag.snapshot(false), 0);
        }

        void reset() {
            attempts.reset(); success.reset(); failures.reset(); bytes.reset(); clockSkew.reset();
            latency.reset(); e2e.reset(); deliveryLag.reset(); lastSnapshot.set(System.nanoTime());
        }
    }

    private static final class Snapshot {
        final long attempts, success, failures, bytes, clockSkew, elapsedNanos;
        final HistogramSnapshot latency, e2e, deliveryLag;

        Snapshot(long attempts, long success, long failures, long bytes, long clockSkew,
            HistogramSnapshot latency, HistogramSnapshot e2e, HistogramSnapshot deliveryLag, long elapsedNanos) {
            this.attempts = attempts; this.success = success; this.failures = failures; this.bytes = bytes;
            this.clockSkew = clockSkew; this.latency = latency; this.e2e = e2e; this.deliveryLag = deliveryLag;
            this.elapsedNanos = elapsedNanos;
        }
    }

    private static final class NamedFactory implements ThreadFactory {
        private final String name;
        NamedFactory(String name) { this.name = name; }
        @Override public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        }
    }
}
