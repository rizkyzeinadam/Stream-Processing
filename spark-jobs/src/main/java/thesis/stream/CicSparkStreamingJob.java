package thesis.stream;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.functions.window;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.streaming.StreamingQuery;
import java.net.InetSocketAddress;
import java.io.OutputStream;
import com.sun.net.httpserver.HttpServer;

public class CicSparkStreamingJob {
    private static final int PARALLELISM = 2;
    private static volatile double currentThroughput = 0.0;
    private static volatile double currentThroughputWorker1 = 0.0;
    private static volatile double currentThroughputWorker2 = 0.0;
    private static volatile double currentLatency = 0.0;
    private static volatile long totalCount = 0;
    private static RecoveryValidator recoveryValidator = new RecoveryValidator();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = args.length > 0 ? args[0] : "kafka:29092";
        String topic = args.length > 1 ? args[1] : "cic-topic";
        String groupId = args.length > 2 ? args[2] : "spark-group";

        HttpServer server = HttpServer.create(new InetSocketAddress(9091), 0);
        server.createContext("/metrics", exchange -> {
            String response = "# HELP spark_streaming_throughput Throughput\n" +
                              "# TYPE spark_streaming_throughput gauge\n" +
                              "spark_streaming_throughput " + currentThroughput + "\n" +
                              "# HELP spark_streaming_throughput_worker1 Throughput mapped to kafka partition 0\n" +
                              "# TYPE spark_streaming_throughput_worker1 gauge\n" +
                              "spark_streaming_throughput_worker1 " + currentThroughputWorker1 + "\n" +
                              "# HELP spark_streaming_throughput_worker2 Throughput mapped to kafka partition 1\n" +
                              "# TYPE spark_streaming_throughput_worker2 gauge\n" +
                              "spark_streaming_throughput_worker2 " + currentThroughputWorker2 + "\n" +
                              "# HELP spark_streaming_latency Latency\n" +
                              "# TYPE spark_streaming_latency gauge\n" +
                              "spark_streaming_latency " + currentLatency + "\n";
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });
        server.setExecutor(null);
        server.start();

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
                .option("startingOffsets", "latest")
                .option("kafka.group.id", groupId)
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr(
                    "CAST(value AS STRING) AS value",
                    "timestamp AS kafka_timestamp",
                    "partition AS kafka_partition"
                )
                .withColumn("process_timestamp", current_timestamp())
                .withColumn(
                        "latency_ms",
                        unix_timestamp(col("process_timestamp")).multiply(1000)
                                .minus(unix_timestamp(col("kafka_timestamp")).multiply(1000))
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
                        try {
                            Row avgLatencyRow = batchDf.selectExpr("avg(latency_ms)").first();
                            double avgLat = avgLatencyRow.isNullAt(0) ? 0.0 : avgLatencyRow.getDouble(0);
                            currentLatency = avgLat;
                        } catch (Exception e) {}
                    }
                    totalCount += count;
                    currentThroughput = count;
                    currentThroughputWorker1 = worker1Count;
                    currentThroughputWorker2 = worker2Count;
                    recoveryValidator.recordMessage(totalCount);
                })
                .option("checkpointLocation", "/tmp/spark-query-checkpoints/data")
                .start();

        dataQuery.awaitTermination();
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
