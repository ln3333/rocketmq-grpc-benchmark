package org.apache.rocketmq.grpc.benchmark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class Options {
    enum Command { PRODUCER, PUSH_CONSUMER, SIMPLE_CONSUMER }

    private static final Set<String> FLAGS = set("help", "key-enabled", "fifo-accelerator");
    private static final Map<String, String> ALIASES;
    private static final Set<String> COMMON = set(
        "help", "endpoints", "namespace", "ssl", "access-key", "secret-key", "security-token",
        "request-timeout-ms", "max-startup-attempts", "duration-seconds", "warmup-seconds",
        "report-interval-seconds", "rate", "csv", "topic", "threads");
    private static final Set<String> PRODUCER = set(
        "message-size", "topic-type", "send-mode", "max-inflight", "tag", "key-enabled", "property-size",
        "max-attempts", "message-groups", "delay-ms", "commit-percent");
    private static final Set<String> CONSUMER = set(
        "consumer-group", "filter-type", "filter-expression");
    private static final Set<String> PUSH = set(
        "max-cache-messages", "max-cache-bytes", "fifo-accelerator");
    private static final Set<String> SIMPLE = set("batch-size", "await-ms", "invisible-ms");

    static {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("t", "topic");
        aliases.put("w", "threads");
        aliases.put("s", "message-size");
        aliases.put("g", "consumer-group");
        aliases.put("h", "help");
        ALIASES = Collections.unmodifiableMap(aliases);
    }

    final Command command;
    final Map<String, String> values;

    private Options(Command command, Map<String, String> values) {
        this.command = command;
        this.values = values;
    }

    static Options parse(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing command: producer, push-consumer, or simple-consumer");
        }
        Command command;
        switch (args[0]) {
            case "producer": command = Command.PRODUCER; break;
            case "push-consumer": command = Command.PUSH_CONSUMER; break;
            case "simple-consumer": command = Command.SIMPLE_CONSUMER; break;
            default: throw new IllegalArgumentException("Unknown command: " + args[0]);
        }

        Map<String, String> values = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("-") || "-".equals(token)) {
                throw new IllegalArgumentException("Unexpected argument: " + token);
            }
            String raw = token.startsWith("--") ? token.substring(2) : token.substring(1);
            String value = null;
            int equals = raw.indexOf('=');
            if (equals >= 0) {
                value = raw.substring(equals + 1);
                raw = raw.substring(0, equals);
            }
            String name = ALIASES.getOrDefault(raw, raw);
            if (FLAGS.contains(name)) {
                if (value == null) value = "true";
            } else if (value == null) {
                if (++i >= args.length || args[i].startsWith("-")) {
                    throw new IllegalArgumentException("Missing value for --" + name);
                }
                value = args[i];
            }
            if (values.put(name, value) != null) {
                throw new IllegalArgumentException("Duplicate option: --" + name);
            }
        }
        Options options = new Options(command, values);
        options.validateKnownOptions();
        if (!options.bool("help", false)) options.validate();
        return options;
    }

    private void validateKnownOptions() {
        Set<String> allowed = new HashSet<>(COMMON);
        if (command == Command.PRODUCER) allowed.addAll(PRODUCER);
        else {
            allowed.addAll(CONSUMER);
            allowed.addAll(command == Command.PUSH_CONSUMER ? PUSH : SIMPLE);
        }
        for (String key : values.keySet()) {
            if (!allowed.contains(key)) throw new IllegalArgumentException("Unknown option: --" + key);
        }
    }

    private void validate() {
        required("endpoints", env("ROCKETMQ_ENDPOINTS"));
        required("topic", null);
        positive("request-timeout-ms", 3000);
        positive("max-startup-attempts", 3);
        nonNegative("duration-seconds", 0);
        nonNegative("warmup-seconds", 10);
        positive("report-interval-seconds", 10);
        nonNegative("rate", 0);
        positive("threads", command == Command.PRODUCER ? 64 : command == Command.PUSH_CONSUMER ? 20 : 4);
        credentials();
        bool("ssl", true);

        if (command == Command.PRODUCER) validateProducer();
        else validateConsumer();
    }

    private void validateProducer() {
        oneOf("topic-type", "normal", "normal", "fifo", "delay", "tx");
        oneOf("send-mode", "sync", "sync", "async");
        positive("message-size", 128);
        positive("max-inflight", 1024);
        nonNegative("property-size", 0);
        positive("max-attempts", 3);
        positive("message-groups", 64);
        nonNegative("delay-ms", 10000);
        percent("commit-percent", 100);
        bool("key-enabled", false);
        if ("tx".equals(get("topic-type", "normal")) && "async".equals(get("send-mode", "sync"))) {
            throw new IllegalArgumentException("Transactional messages only support --send-mode sync");
        }
    }

    private void validateConsumer() {
        required("consumer-group", null);
        oneOf("filter-type", "tag", "tag", "sql92");
        if (command == Command.PUSH_CONSUMER) {
            positive("max-cache-messages", 4096);
            positive("max-cache-bytes", 64 * 1024 * 1024);
            bool("fifo-accelerator", false);
        } else {
            positive("batch-size", 16);
            positive("await-ms", 30000);
            positive("invisible-ms", 15000);
        }
    }

    private void credentials() {
        String access = getOrEnv("access-key", "ROCKETMQ_ACCESS_KEY");
        String secret = getOrEnv("secret-key", "ROCKETMQ_SECRET_KEY");
        String token = getOrEnv("security-token", "ROCKETMQ_SECURITY_TOKEN");
        if ((access == null) != (secret == null)) {
            throw new IllegalArgumentException("Access key and secret key must be configured together");
        }
        if (token != null && access == null) {
            throw new IllegalArgumentException("Security token requires access key and secret key");
        }
    }

    String endpoints() { return getOrEnv("endpoints", "ROCKETMQ_ENDPOINTS"); }
    String accessKey() { return getOrEnv("access-key", "ROCKETMQ_ACCESS_KEY"); }
    String secretKey() { return getOrEnv("secret-key", "ROCKETMQ_SECRET_KEY"); }
    String securityToken() { return getOrEnv("security-token", "ROCKETMQ_SECURITY_TOKEN"); }
    String get(String name, String defaultValue) { return values.getOrDefault(name, defaultValue); }
    int integer(String name, int defaultValue) { return Integer.parseInt(get(name, String.valueOf(defaultValue))); }
    long longValue(String name, long defaultValue) { return Long.parseLong(get(name, String.valueOf(defaultValue))); }
    boolean bool(String name, boolean defaultValue) {
        String value = get(name, String.valueOf(defaultValue));
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("--" + name + " must be true or false");
        }
        return Boolean.parseBoolean(value);
    }

    private String getOrEnv(String name, String envName) {
        String value = values.get(name);
        return value != null && !value.isEmpty() ? value : env(envName);
    }

    private void required(String name, String fallback) {
        String value = values.get(name);
        if ((value == null || value.isEmpty()) && (fallback == null || fallback.isEmpty())) {
            throw new IllegalArgumentException("Missing required option: --" + name);
        }
    }

    private void positive(String name, int defaultValue) {
        if (integer(name, defaultValue) <= 0) throw new IllegalArgumentException("--" + name + " must be > 0");
    }

    private void nonNegative(String name, long defaultValue) {
        if (longValue(name, defaultValue) < 0) throw new IllegalArgumentException("--" + name + " must be >= 0");
    }

    private void percent(String name, int defaultValue) {
        int value = integer(name, defaultValue);
        if (value < 0 || value > 100) throw new IllegalArgumentException("--" + name + " must be in [0, 100]");
    }

    private void oneOf(String name, String defaultValue, String... accepted) {
        String value = get(name, defaultValue).toLowerCase();
        if (!Arrays.asList(accepted).contains(value)) {
            throw new IllegalArgumentException("--" + name + " must be one of " + Arrays.toString(accepted));
        }
    }

    static String help(Command command) {
        String common = "Common:\n"
            + "  --endpoints HOST:PORT       RocketMQ Proxy gRPC endpoint (or ROCKETMQ_ENDPOINTS)\n"
            + "  --topic, -t TOPIC           Topic name\n"
            + "  --threads, -w N             Worker threads\n"
            + "  --namespace NAME            Resource namespace\n"
            + "  --ssl true|false            Enable TLS (default: true)\n"
            + "  --access-key/--secret-key   ACL credentials (environment variables also supported)\n"
            + "  --security-token TOKEN      Optional session token\n"
            + "  --request-timeout-ms N      RPC timeout (default: 3000)\n"
            + "  --max-startup-attempts N    Client startup attempts (default: 3)\n"
            + "  --warmup-seconds N          Excluded warmup (default: 10)\n"
            + "  --duration-seconds N        Measured duration; 0 runs until signal (default: 0)\n"
            + "  --report-interval-seconds N Report interval (default: 10)\n"
            + "  --rate N                    Global messages/second limit; 0 is unlimited\n"
            + "  --csv PATH                  Write interval snapshots to CSV\n";
        if (command == Command.PRODUCER) {
            return "Usage: producer.sh [options]\n" + common
                + "Producer:\n"
                + "  --message-size, -s N       Body bytes (default: 128)\n"
                + "  --topic-type TYPE          normal|fifo|delay|tx (default: normal)\n"
                + "  --send-mode MODE           sync|async (default: sync; tx is sync only)\n"
                + "  --max-inflight N           Async request limit (default: 1024)\n"
                + "  --tag TAG                  Optional message tag\n"
                + "  --key-enabled              Generate a unique key per message\n"
                + "  --property-size N          Benchmark property bytes (default: 0)\n"
                + "  --max-attempts N           SDK send attempts (default: 3)\n"
                + "  --message-groups N         FIFO group count (default: 64)\n"
                + "  --delay-ms N               Delay delivery offset (default: 10000)\n"
                + "  --commit-percent N         Transaction commit percentage (default: 100)\n";
        }
        String consumer = "Consumer:\n"
            + "  --consumer-group, -g GROUP Consumer group\n"
            + "  --filter-type TYPE          tag|sql92 (default: tag)\n"
            + "  --filter-expression EXPR    Filter expression (default: *)\n";
        if (command == Command.PUSH_CONSUMER) {
            return "Usage: push-consumer.sh [options]\n" + common + consumer
                + "  --max-cache-messages N     Local cache count (default: 4096)\n"
                + "  --max-cache-bytes N        Local cache bytes (default: 67108864)\n"
                + "  --fifo-accelerator         Enable FIFO consume accelerator\n";
        }
        return "Usage: simple-consumer.sh [options]\n" + common + consumer
            + "  --batch-size N             Receive batch size (default: 16)\n"
            + "  --await-ms N               Long-poll duration (default: 30000)\n"
            + "  --invisible-ms N           Message invisibility (default: 15000)\n";
    }

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? null : value;
    }

    private static Set<String> set(String... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }
}
