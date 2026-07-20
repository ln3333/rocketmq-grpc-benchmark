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

/**
 * Append-only per-message evidence for offline reconciliation.
 * Producer: {@code sent}; consumer: {@code recv} then {@code done}.
 */
final class MessageJournal implements Closeable {
    private final BufferedWriter writer;
    private final Object lock = new Object();

    MessageJournal(String path, String role, String instanceId) throws IOException {
        Path file = Paths.get(path);
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        this.writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        writer.write("# rocketmq-grpc-benchmark journal v1 role=" + role + " instance=" + instanceId);
        writer.newLine();
        writer.write("# timestamp,event,key,seq,sendTs,messageId,statusOrExtraTs,duplicate");
        writer.newLine();
        writer.flush();
    }

    void sent(String key, long seq, long sendTs, String messageId, boolean ok) {
        write("sent", key, seq, sendTs, messageId == null ? "-" : messageId, ok ? "ok" : "fail", "");
    }

    void received(String key, long seq, long sendTs, String messageId, long recvTs) {
        write("recv", key, seq, sendTs, messageId, Long.toString(recvTs), "");
    }

    void done(String key, long seq, long sendTs, String messageId, long doneTs, boolean duplicate) {
        write("done", key, seq, sendTs, messageId, Long.toString(doneTs), duplicate ? "dup" : "unique");
    }

    private void write(String event, String key, long seq, long sendTs, String messageId,
        String statusOrExtraTs, String duplicate) {
        String line = Instant.now() + "," + event + "," + escape(key) + "," + seq + "," + sendTs + ","
            + escape(messageId) + "," + statusOrExtraTs + "," + duplicate;
        synchronized (lock) {
            try {
                writer.write(line);
                writer.newLine();
            } catch (IOException e) {
                System.err.println("Failed to write message journal: " + e);
            }
        }
    }

    void flush() {
        synchronized (lock) {
            try {
                writer.flush();
            } catch (IOException e) {
                System.err.println("Failed to flush message journal: " + e);
            }
        }
    }

    private static String escape(String value) {
        if (value == null) return "";
        if (value.indexOf(',') < 0 && value.indexOf('\n') < 0 && value.indexOf('\r') < 0) return value;
        return value.replace('\n', ' ').replace('\r', ' ').replace(',', '_');
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            writer.flush();
            writer.close();
        }
    }
}
