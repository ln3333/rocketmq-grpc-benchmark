package org.apache.rocketmq.grpc.benchmark;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.client.apis.message.MessageView;

/** Client-side message identity used for local duplicate detection and offline journals. */
final class MessageTrace {
    static final String SEQ_PROPERTY = "benchmarkSeq";
    static final String SEND_TS_PROPERTY = "benchmarkSendTs";

    private MessageTrace() { }

    static String newInstanceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    static String key(String instanceId, long seq) {
        return "benchmark-" + instanceId + "-" + seq;
    }

    static TraceInfo inspect(MessageView message, Set<String> seenKeys) {
        Map<String, String> properties = message.getProperties();
        String key = firstKey(message.getKeys());
        if (key == null || key.isEmpty()) {
            key = message.getMessageId().toString();
        }
        long seq = parseLong(properties.get(SEQ_PROPERTY), -1L);
        long sendTs = parseLong(properties.get(SEND_TS_PROPERTY), -1L);
        boolean duplicate = !seenKeys.add(key);
        return new TraceInfo(key, seq, sendTs, message.getMessageId().toString(), duplicate);
    }

    static Long e2eMillis(MessageView message, long nowMillis) {
        long sendTs = parseLong(message.getProperties().get(SEND_TS_PROPERTY), -1L);
        if (sendTs >= 0) return nowMillis - sendTs;
        return nowMillis - message.getBornTimestamp();
    }

    private static String firstKey(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) return null;
        return keys.iterator().next();
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null || value.isEmpty()) return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    static final class TraceInfo {
        final String key;
        final long seq;
        final long sendTs;
        final String messageId;
        final boolean duplicate;

        TraceInfo(String key, long seq, long sendTs, String messageId, boolean duplicate) {
            this.key = key;
            this.seq = seq;
            this.sendTs = sendTs;
            this.messageId = messageId;
            this.duplicate = duplicate;
        }
    }
}
