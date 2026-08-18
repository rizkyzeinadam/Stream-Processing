package thesis.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.sun.net.httpserver.HttpServer;

import static org.apache.spark.sql.functions.current_timestamp;

public class CicSparkStreamingJob {
    private static final int PARALLELISM = 2;
    private static final long LATENCY_WINDOW_MS = 5000L;
    private static final Deque<double[]> latencySamples = new ArrayDeque<>();

    // Kept only for the existing unit test; the streaming latency metric does
    // NOT use payload field parsing. It uses the Kafka message timestamp instead.
    private static final Pattern EVENT_TIME_PATTERN = Pattern.compile("\\\"event_time_ms\\\"\\s*:\\s*(\\d+)");

    private static volatile double currentThroughput = 0.0;
    private static volatile double currentThroughputWorker1 = 0.0;
    private static volatile double currentThroughputWorker2 = 0.0;
    private static volatile double currentLatency = 0.0;
    private static volatile long totalCount = 0;
    private static volatile long lastBatchStart = 0L;

    // Backlog is computed from true Kafka end offsets, not from throughput.
    private static String bootstrapServersStatic = null;
    private static String topicStatic = null;
    private static KafkaConsumer<String, String> offsetsConsumer = null;
    private static volatile double currentBacklog = 0.0;

    private static RecoveryValidator recoveryValidator = new RecoveryValidator();

    public static double computeLatencyMs(String payload, long processingTimeMs) {
        if (payload == null || payload.isEmpty()) {
            return 0.0;
        }

        Matcher matcher = EVENT_TIME_PATTERN.matcher(payload);
        if (!matcher.find()) {
            return 0.0;
        }

        try {
            String rawNumber = matcher.group(1).replace("_", "").replace("L", "").replace("l", "");
            long eventTimeMs = Long.parseLong(rawNumber);
            return Math.max(0.0, processingTimeMs - eventTimeMs);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : "kafka:29092";
        String topic = args.length > 1 ? args[1] : "cic-topic";
        String groupId = args.length > 2 ? args[2] : "spark-group";

        bootstrapServersStatic = bootstrapServers;
        topicStatic = topic;

        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(9091), 0);
        } catch (java.io.IOException bindEx) {
            System.out.println("[METRICS] WARN: port 9091 already in use by a previous job instance; "
                + "metrics endpoint will NOT be exposed for this run. Kill stale processes and resubmit: "
                + "docker exec spark-driver bash -lc \"pkill -9 -f CicSparkStreamingJob; pkill -9 -f SparkSubmit\"");
        }

        if (server != null) {
            server.createContext("/metrics", exchange -> {
            currentBacklog = computeBacklog(totalCount);
            String response = "# HELP spark_streaming_throughput Throughput\n" +
                              "# TYPE spark_streaming_throughput gauge\n" +
                              "spark_streaming_throughput " + currentThroughput + "\n" +
                              "# HELP spark_streaming_throughput_worker1 Throughput mapped to kafka partition 0\n" +
                              "# TYPE spark_streaming_throughput_worker1 gauge\n" +
                              "spark_streaming_throughput_worker1 " + currentThroughputWorker1 + "\n" +
                              "# HELP spark_streaming_throughput_worker2 Throughput mapped to kafka partition 1\n" +
                              "# TYPE spark_streaming_throughput_worker2 gauge\n" +
                              "spark_streaming_throughput_worker2 " + currentThroughputWorker2 + "\n" +
                              "# HELP spark_streaming_latency Latency in milliseconds\n" +
                              "# TYPE spark_streaming_latency gauge\n" +
                              "spark_streaming_latency " + currentLatency + "\n" +
                              "# HELP spark_streaming_latency_average Average latency in milliseconds\n" +
                              "# TYPE spark_streaming_latency_average gauge\n" +
                              "spark_streaming_latency_average " + currentLatency + "\n" +
                              "# HELP spark_streaming_backlog Unprocessed records (kafka end offset - processed)\n" +
                              "# TYPE spark_streaming_backlog gauge\n" +
                              "spark_streaming_backlog " + currentBacklog + "\n";
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
            });
            server.setExecutor(null);
            server.start();
        }

        SparkSession spark = SparkSession.builder()
                .appName("CIC Spark Streaming Job")
                .config("spark.sql.shuffle.partitions", String.valueOf(PARALLELISM))
                .config("spark.default.parallelism", String.valueOf(PARALLELISM))
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> kafkaInput = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("kafka.group.id", groupId)
                .option("failOnDataLoss", "true")
                .load()
                .selectExpr(
                    "CAST(value AS STRING) AS value",
                    "timestamp AS kafka_timestamp",
                    "partition AS kafka_partition"
                )
                .withColumn("process_timestamp", current_timestamp())
                .withColumn(
                        "latency_ms",
                        org.apache.spark.sql.functions.expr(
                            "CASE WHEN kafka_timestamp IS NOT NULL THEN (unix_micros(process_timestamp) - unix_micros(kafka_timestamp)) / 1000.0 ELSE 0.0 END"
                        )
                );

        StreamingQuery dataQuery = kafkaInput.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDf, batchId) -> {
                    long count = batchDf.count();
                    double worker1Count = 0.0;
                    double worker2Count = 0.0;

                    for (Row row : batchDf.groupBy("kafka_partition").count().collectAsList()) {
                        int partition = row.getInt(0);
                        double partitionCount = row.getLong(1);
                        if (partition == 0) {
                            worker1Count += partitionCount;
                        } else if (partition == 1) {
                            worker2Count += partitionCount;
                        }
                    }

                    if (count > 0) {
                        Row avgLatencyRow = batchDf.selectExpr("avg(latency_ms)").first();
                        Row sample = batchDf.select("kafka_timestamp", "process_timestamp", "latency_ms").first();
                        System.out.println("[LATENCY DEBUG] count=" + count
                            + " kafka_ts=" + String.valueOf(sample.get(0))
                            + " process_ts=" + String.valueOf(sample.get(1))
                            + " latency_ms=" + String.valueOf(sample.get(2))
                            + " avg_null=" + avgLatencyRow.isNullAt(0));
                        double avgLat = 0.0;
                        if (!avgLatencyRow.isNullAt(0)) {
                            Object avgVal = avgLatencyRow.get(0);
                            if (avgVal instanceof java.math.BigDecimal) {
                                avgLat = ((java.math.BigDecimal) avgVal).doubleValue();
                            } else if (avgVal instanceof Number) {
                                avgLat = ((Number) avgVal).doubleValue();
                            }
                        }
                        if (!Double.isNaN(avgLat)) currentLatency = avgLat;
                    }
                    totalCount += count;
                    long now = System.currentTimeMillis();
                    double intervalSec = (lastBatchStart > 0) ? (now - lastBatchStart) / 1000.0 : 1.0;
                    lastBatchStart = now;
                    currentThroughput = (double) count / Math.max(intervalSec, 1e-3);
                    currentThroughputWorker1 = worker1Count;
                    currentThroughputWorker2 = worker2Count;
                    recoveryValidator.recordMessage(totalCount);
                })
                .option("checkpointLocation", "file:///tmp/spark-query-checkpoints/data")
                .start();

        dataQuery.awaitTermination();
    }

    private static synchronized double updateLatencyWindow(double batchLatencyMs) {
        long now = System.currentTimeMillis();
        latencySamples.addLast(new double[]{now, batchLatencyMs});
        while (!latencySamples.isEmpty() && (now - latencySamples.peekFirst()[0]) > LATENCY_WINDOW_MS) {
            latencySamples.removeFirst();
        }
        if (latencySamples.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (double[] sample : latencySamples) {
            sum += sample[1];
        }
        return sum / latencySamples.size();
    }

    private static synchronized double computeBacklog(long processedTotal) {
        if (bootstrapServersStatic == null || topicStatic == null) {
            return currentBacklog;
        }
        try {
            if (offsetsConsumer == null) {
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersStatic);
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer");
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        "org.apache.kafka.common.serialization.StringDeserializer");
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
                props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
                offsetsConsumer = new KafkaConsumer<>(props);
            }

            List<PartitionInfo> infos = offsetsConsumer.partitionsFor(topicStatic);
            if (infos == null || infos.isEmpty()) {
                return currentBacklog;
            }
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo info : infos) {
                tps.add(new TopicPartition(info.topic(), info.partition()));
            }
            Map<TopicPartition, Long> ends = offsetsConsumer.endOffsets(tps);
            long latest = 0L;
            for (long v : ends.values()) {
                latest += v;
            }
            currentBacklog = Math.max(0.0, latest - processedTotal);
        } catch (Exception e) {
            // Keep the previous backlog value on any transient Kafka error.
        }
        return currentBacklog;
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
    }
}