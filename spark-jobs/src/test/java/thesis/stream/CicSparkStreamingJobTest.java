package thesis.stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CicSparkStreamingJobTest {
    @Test
    void shouldComputeLatencyFromProducerEventTime() {
        long processingTimeMs = 1_700_000_000_000L;
        String payload = "{\"event_time_ms\": 1699999999000, \"message_id\": 42}";

        double latencyMs = CicSparkStreamingJob.computeLatencyMs(payload, processingTimeMs);

        assertEquals(1000.0, latencyMs, 0.01);
    }
}
