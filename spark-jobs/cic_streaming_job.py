#!/usr/bin/env python3
"""
CIC Streaming Job for Spark
Reads from Kafka topic, processes records with parallelism 2, and tracks metrics
Equivalent flow to Flink job for fair comparison
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json,
    window, count, avg, max, min,
    current_timestamp,
    unix_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def setup_spark_session(bootstrap_servers, topic, group_id):
    """Create and configure Spark session with metrics"""
    spark = SparkSession.builder \
        .appName("CIC Kafka Streaming Job") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.metrics.conf", "spark-metrics.conf") \
        .getOrCreate()
    
    # Set parallelism
    spark.sparkContext.setDefaultParallelism(2)
    spark.sql("SET spark.sql.shuffle.partitions=2")
    
    # Enable verbose logging for debugging
    spark.sparkContext.setLogLevel("INFO")
    
    return spark, bootstrap_servers, topic, group_id

def create_kafka_source(spark, bootstrap_servers, topic, group_id):
    """Create Kafka source with proper schema inference"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Cast value to string and add timestamp
    df = df.select(
        col("value").cast("string").alias("json_value"),
        col("timestamp").alias("kafka_timestamp")
    )
    
    return df

def process_stream(df, bootstrap_servers, topic, group_id):
    """Transform stream: parse JSON, extract fields, calculate metrics"""
    try:
        # Sample schema to help with parsing
        sample_schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("src_ip", StringType(), True),
            StructField("dst_ip", StringType(), True),
            StructField("proto", StringType(), True),
            StructField("duration", DoubleType(), True),
        ])
        
        # Parse JSON and track records
        df_parsed = df.select(
            from_json(col("json_value"), sample_schema).alias("data"),
            col("kafka_timestamp")
        ).select(
            col("data.*"),
            col("kafka_timestamp"),
            current_timestamp().alias("process_timestamp")
        )
        
        # Calculate latency
        df_with_latency = df_parsed.withColumn(
            "latency_ms",
            (unix_timestamp(col("process_timestamp")) * 1000 - 
             unix_timestamp(col("kafka_timestamp")) * 1000).cast("long")
        )
        
        # Window aggregation for metrics (1-minute windows)
        metrics = df_with_latency.groupBy(
            window(col("kafka_timestamp"), "1 minute")
        ).agg(
            count("*").alias("record_count"),
            avg("latency_ms").alias("avg_latency_ms"),
            max("latency_ms").alias("max_latency_ms"),
            min("latency_ms").alias("min_latency_ms")
        )
        
        return df_with_latency, metrics
        
    except Exception as e:
        print(f"[ERROR] Stream processing failed: {e}")
        raise

def write_stream_to_noop(df, checkpoint_location="/tmp/spark-query-checkpoints/noop"):
    """Trigger stream processing without heavy console output"""
    def _consume_batch(batch_df, batch_id):
        _ = batch_df.count()

    query = df.writeStream \
        .foreachBatch(_consume_batch) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    return query

def write_metrics_stream(metrics_df, checkpoint_location="/tmp/spark-metrics-checkpoints"):
    """Trigger metrics aggregation without large logs"""
    def _consume_metrics(batch_df, batch_id):
        _ = batch_df.count()

    query = metrics_df.writeStream \
        .foreachBatch(_consume_metrics) \
        .option("checkpointLocation", checkpoint_location) \
        .start()

    return query

def main():
    """Main execution"""
    # Parse arguments
    bootstrap_servers = sys.argv[1] if len(sys.argv) > 1 else "kafka:29092"
    topic = sys.argv[2] if len(sys.argv) > 2 else "cic-topic"
    group_id = sys.argv[3] if len(sys.argv) > 3 else "spark-group"
    
    print(f"[SPARK JOB] Starting CIC Streaming Job")
    print(f"  Bootstrap Servers: {bootstrap_servers}")
    print(f"  Topic: {topic}")
    print(f"  Consumer Group: {group_id}")
    print(f"  Parallelism: 2")
    
    # Setup Spark session
    spark, bs, tp, gid = setup_spark_session(bootstrap_servers, topic, group_id)
    
    # Create Kafka source
    print("[SPARK JOB] Creating Kafka source...")
    df_kafka = create_kafka_source(spark, bs, tp, gid)
    
    # Process stream
    print("[SPARK JOB] Processing stream...")
    df_processed, metrics_df = process_stream(df_kafka, bs, tp, gid)
    
    # Write output streams
    print("[SPARK JOB] Starting stream writers...")
    query_data = write_stream_to_noop(df_processed)
    query_metrics = write_metrics_stream(metrics_df)
    
    # Wait for both queries
    print("[SPARK JOB] Waiting for stream completion...")
    spark.streams.awaitAnyTermination()
    
    print("[SPARK JOB] Job completed")

if __name__ == "__main__":
    main()
