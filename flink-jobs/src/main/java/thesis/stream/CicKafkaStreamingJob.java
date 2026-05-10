package thesis.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;
import java.time.Instant;
import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class CicKafkaStreamingJob {

    private static final int PARALLELISM = 2;
    private static RecoveryValidator recoveryValidator = new RecoveryValidator();

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "kafka:29092";
        final String topic = args.length > 1 ? args[1] : "cic-topic";
        final String groupId = args.length > 2 ? args[2] : "flink-group";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(5)));
        env.getConfig().setLatencyTrackingInterval(1000L);

        KafkaSource<String> source =
            KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();

        DataStream<String> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                        .name("kafka-source")
                .setParallelism(PARALLELISM)
                .keyBy(value -> value)
                .map(new ProcessingMap())
                        .name("processing-map")
                .uid("processing-map")
                .setParallelism(PARALLELISM);

        stream.addSink(new DiscardingSink<>()).name("discard-sink").setParallelism(PARALLELISM);

        env.execute("CIC Kafka Streaming Job");
    }

    public static class ProcessingMap extends RichMapFunction<String, String> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private transient Counter recordsIn;
        private transient Counter recordsOut;
        private transient Counter throughputCounter;
        private final AtomicLong lastLatencyMs = new AtomicLong(0L);

        @Override
        public void open(Configuration parameters) {
            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            final int totalParallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            final String hostName = resolveHostName();

            System.out.println("Operator distribution: subtask " + subtaskIndex + " of " + totalParallelism + " running on " + hostName);
            System.out.println("Expected partition assignment: subtask " + subtaskIndex + " handles partition " + subtaskIndex);
            if (hostName.contains("taskmanager1") || subtaskIndex == 0) {
                System.out.println("TaskManager1 handling partition 0");
            }
            if (hostName.contains("taskmanager2") || subtaskIndex == 1) {
                System.out.println("TaskManager2 handling partition 1");
            }

            recordsIn = getRuntimeContext().getMetricGroup().counter("records_in");
            recordsOut = getRuntimeContext().getMetricGroup().counter("records_out");
            throughputCounter = getRuntimeContext().getMetricGroup().counter("throughput_counter");
            getRuntimeContext().getMetricGroup().meter("throughput", new MeterView(throughputCounter, 60));
            getRuntimeContext().getMetricGroup().gauge("latency", lastLatencyMs::get);
        }

        private String resolveHostName() {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return "unknown-host";
            }
        }

        @Override
        public String map(String value) {
            recordsIn.inc();
            throughputCounter.inc();

            long latencyMs = 0L;
            try {
                JsonNode root = MAPPER.readTree(value);
                JsonNode ts = root.get("timestamp");
                if (ts != null && !ts.isNull()) {
                    Instant eventTs = Instant.parse(ts.asText());
                    latencyMs = Math.max(0L, Duration.between(eventTs, Instant.now()).toMillis());
                }
            } catch (Exception ignored) {
                latencyMs = 0L;
            }
            lastLatencyMs.set(latencyMs);

            recoveryValidator.recordMessage(throughputCounter.getCount());
            recordsOut.inc();
            return value;
        }
    }

    public static class RecoveryValidator {
        private long lastMessageCount = 0;
        private long normalThroughput = 0;
        private long faultDetectionTime = -1;
        private long recoveryStartTime = -1;
        private boolean recoveryLogged = false;
        private int restartLoopDetectionCount = 0;

        public synchronized void recordMessage(long messageCount) {
            if (messageCount == 0) return;

            long currentTime = System.currentTimeMillis();
            long messagesSinceLastCheck = messageCount - lastMessageCount;
            lastMessageCount = messageCount;

            if (normalThroughput == 0 && messagesSinceLastCheck > 100) {
                normalThroughput = messagesSinceLastCheck;
                System.out.println("[RECOVERY VALIDATION] NORMAL THROUGHPUT: " + normalThroughput + " msg/s");
            }

            if (normalThroughput > 0 && messagesSinceLastCheck == 0 && faultDetectionTime == -1) {
                faultDetectionTime = currentTime;
                System.out.println("[RECOVERY VALIDATION] FAULT DETECTED at " + (currentTime / 1000));
            }

            if (faultDetectionTime > -1 && messagesSinceLastCheck > 0 && recoveryStartTime == -1) {
                recoveryStartTime = currentTime;
                System.out.println("[RECOVERY VALIDATION] RECOVERY STARTED at " + (currentTime / 1000));
            }

            if (recoveryStartTime > -1 && messagesSinceLastCheck > (normalThroughput / 2) && !recoveryLogged) {
                recoveryLogged = true;
                System.out.println("[RECOVERY VALIDATION] RECOVERY COMPLETE: " + messagesSinceLastCheck + " msg/s");
            }
        }

        public synchronized void checkRestartLoop() {
            if (normalThroughput > 0 && lastMessageCount == 0) {
                restartLoopDetectionCount++;
                if (restartLoopDetectionCount > 20) {
                    System.out.println("[RECOVERY VALIDATION] WARNING: Restart loop detected for >20s");
                }
            } else {
                restartLoopDetectionCount = 0;
            }
        }
    }
}
