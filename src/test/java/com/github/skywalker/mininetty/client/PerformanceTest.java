package com.github.skywalker.mininetty.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

/**
 * Client round-trip performance tests: one or more clients connect to an echo server
 * and synchronously "send a line, read a line", counting completions within a fixed
 * time window (i.e. requests per second / RTT-TPS) along with round-trip latency
 * percentiles.
 *
 * <ul>
 *   <li>{@link #testSingleConnectionRoundTrip()}: single-connection throughput/latency baseline;</li>
 *   <li>{@link #testConcurrentClientsRoundTrip()}: by default 3 clients stress-test the same
 *       server concurrently, reporting total system throughput, per-connection throughput,
 *       and merged latency percentiles.</li>
 * </ul>
 *
 * <p><b>Metric definition</b>: these tests measure RTT and TPS under synchronous
 * request-response, so the throughput ceiling is bounded by the per-connection
 * round-trip latency - the most basic performance metric. To measure higher raw
 * throughput, one would switch to batch (pipelined) sending followed by batch reads.</p>
 *
 * <p><b>Tuning parameters</b> (all optional system properties):</p>
 * <ul>
 *   <li>{@code -Dperf.clients=3} number of concurrent client connections</li>
 *   <li>{@code -Dperf.warmup=2000} number of warm-up rounds (triggers JIT, not counted)</li>
 *   <li>{@code -Dperf.duration=3000} length of the timed window in milliseconds</li>
 *   <li>{@code -Dperf.payload=32} payload size in bytes per message</li>
 * </ul>
 * Example: {@code mvn -Dtest=client.PerformanceTest -Dperf.clients=3 -Dperf.duration=10000 test}
 *
 * <p><b>Note</b>: because this class performs continuous reads/writes, the server's idle
 * detection (default 5s) will not close the connections, so longer windows are safe.
 * The numbers are one-off observations subject to local machine load; no numeric
 * assertions are made - only a basic smoke assertion (that requests actually completed
 * within the window).</p>
 *
 * @author skywalker
 */
public class PerformanceTest {

    /** Number of concurrent client connections in the stress test. */
    private static final int CONCURRENT_CLIENTS = Integer.getInteger("perf.clients", 3);
    private static final int WARMUP_ROUNDS = Integer.getInteger("perf.warmup", 2000);
    private static final long DURATION_MILLIS = Long.getLong("perf.duration", 3000L);
    private static final int PAYLOAD_LENGTH = Integer.getInteger("perf.payload", 32);

    @Test
    public void testSingleConnectionRoundTrip() throws Exception {
        String message = buildPayload();
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            // Warm-up: triggers JIT and brings the connection/handler chain into a
            // steady state; not counted in the statistics
            for (int i = 0; i < WARMUP_ROUNDS; i++) {
                client.writeLine(message);
                Assert.assertNotNull("Connection closed by the server during warm-up.", client.readLine());
            }

            long startTime = System.nanoTime();
            long deadline = startTime + TimeUnit.MILLISECONDS.toNanos(DURATION_MILLIS);
            long[] latencies = new long[64];
            int count = 0;
            long sumNanos = 0L;
            while (System.nanoTime() < deadline) {
                long start = System.nanoTime();
                client.writeLine(message);
                // The server echoes each message; a mismatch indicates data cross-talk or misalignment
                Assert.assertEquals(message, client.readLine());
                long rttNanos = System.nanoTime() - start;
                sumNanos += rttNanos;
                if (count == latencies.length) {
                    latencies = Arrays.copyOf(latencies, latencies.length * 2);
                }
                latencies[count++] = rttNanos;
            }

            report(count, sumNanos, System.nanoTime() - startTime, latencies);
            Assert.assertTrue("No requests completed within the window; performance result unusable.", count > 0);
        }
    }

    /**
     * Concurrent stress test: {@link #CONCURRENT_CLIENTS} clients connect to the same
     * server at the same time, warm up independently, and exchange messages synchronously
     * within a shared time window, verifying the server's throughput and latency under
     * concurrent connections.
     */
    @Test
    public void testConcurrentClientsRoundTrip() throws Exception {
        final String message = buildPayload();
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
            try {
                List<Future<WorkerResult>> futures = new ArrayList<>();
                for (int i = 0; i < CONCURRENT_CLIENTS; i++) {
                    futures.add(pool.submit(() -> runConcurrentClient(server.port, message)));
                }
                List<WorkerResult> results = new ArrayList<>(CONCURRENT_CLIENTS);
                int totalCount = 0;
                long totalSumNanos = 0L;
                long minStart = Long.MAX_VALUE;
                long maxEnd = Long.MIN_VALUE;
                for (Future<WorkerResult> future : futures) {
                    // Any per-client assertion/IO exception propagates through the
                    // Future and fails this test
                    WorkerResult result = future.get();
                    results.add(result);
                    totalCount += result.count;
                    totalSumNanos += result.sumNanos;
                    minStart = Math.min(minStart, result.startNanos);
                    maxEnd = Math.max(maxEnd, result.endNanos);
                }
                reportConcurrent(results, totalCount, totalSumNanos, maxEnd - minStart);
                Assert.assertTrue("No requests completed within the window; concurrent result unusable.", totalCount > 0);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    /**
     * The execution body of one concurrent stress-test client: warms up first without
     * timing, then synchronously "sends a line, reads a line" within a fixed time window
     * while collecting statistics.
     */
    private static WorkerResult runConcurrentClient(int port, String message) throws IOException {
        try (TestSupport.ClientConnection client = TestSupport.connect(port)) {
            for (int i = 0; i < WARMUP_ROUNDS; i++) {
                client.writeLine(message);
                Assert.assertNotNull("Connection closed by the server during warm-up.", client.readLine());
            }
            long startNanos = System.nanoTime();
            long deadline = startNanos + TimeUnit.MILLISECONDS.toNanos(DURATION_MILLIS);
            long[] latencies = new long[64];
            int count = 0;
            long sumNanos = 0L;
            while (System.nanoTime() < deadline) {
                long start = System.nanoTime();
                client.writeLine(message);
                // The server echoes each message; a mismatch indicates data cross-talk or misalignment
                Assert.assertEquals(message, client.readLine());
                long rttNanos = System.nanoTime() - start;
                sumNanos += rttNanos;
                if (count == latencies.length) {
                    latencies = Arrays.copyOf(latencies, latencies.length * 2);
                }
                latencies[count++] = rttNanos;
            }
            return new WorkerResult(count, sumNanos, startNanos, System.nanoTime(), latencies);
        }
    }

    /** Statistics of a single stress-test client; startNanos/endNanos delimit its actual timed window. */
    private static final class WorkerResult {

        final int count;
        final long sumNanos;
        final long startNanos;
        final long endNanos;
        final long[] latencies;

        WorkerResult(int count, long sumNanos, long startNanos, long endNanos, long[] latencies) {
            this.count = count;
            this.sumNanos = sumNanos;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.latencies = latencies;
        }
    }

    private static String buildPayload() {
        StringBuilder sb = new StringBuilder(PAYLOAD_LENGTH);
        for (int i = 0; i < PAYLOAD_LENGTH; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    /**
     * Prints throughput and latency statistics: TPS = count / elapsed time; latency
     * percentiles are taken from the sorted samples.
     */
    private static void report(int count, long sumNanos, long elapsedNanos, long[] latencies) {
        long[] samples = Arrays.copyOf(latencies, count);
        Arrays.sort(samples);
        long elapsedMillis = Math.max(1, elapsedNanos / 1_000_000);
        double tps = count * 1000.0 / elapsedMillis;
        double avgMicros = sumNanos / 1000.0 / count;
        System.out.println("======== Single-connection round-trip performance ========");
        System.out.printf("payload=%dB, warmup=%d, elapsed=%dms, requests=%d%n",
                PAYLOAD_LENGTH, WARMUP_ROUNDS, elapsedMillis, count);
        System.out.printf("throughput : %,.0f req/s (RTT-TPS)%n", tps);
        System.out.printf("latency avg: %.3f us%n", avgMicros);
        System.out.printf("latency    : p50=%.1f us, p90=%.1f us, p99=%.1f us, max=%.1f us%n",
                percentileMicros(samples, 50), percentileMicros(samples, 90),
                percentileMicros(samples, 99), samples[samples.length - 1] / 1000.0);
    }

    /**
     * Prints the concurrent test results: aggregate system throughput = total requests
     * divided by the overall span covered by the per-client timed windows; latency
     * percentiles are computed from all clients' samples merged and sorted.
     */
    private static void reportConcurrent(List<WorkerResult> results, int totalCount,
                                         long totalSumNanos, long windowNanos) {
        long windowMillis = Math.max(1, windowNanos / 1_000_000);
        double totalTps = totalCount * 1000.0 / windowMillis;
        double avgMicros = totalSumNanos / 1000.0 / totalCount;
        System.out.println("======== Concurrent " + results.size() + "-client round-trip performance ========");
        System.out.printf("payload=%dB, warmup=%d, window=%dms, total requests=%d%n",
                PAYLOAD_LENGTH, WARMUP_ROUNDS, windowMillis, totalCount);
        for (int i = 0; i < results.size(); i++) {
            WorkerResult result = results.get(i);
            long elapsedMillis = Math.max(1, (result.endNanos - result.startNanos) / 1_000_000);
            double clientTps = result.count * 1000.0 / elapsedMillis;
            System.out.printf("client-%d  : %,d req/s (%d requests)%n", i, (long) clientTps, result.count);
        }
        System.out.printf("aggregate  : %,.0f req/s total%n", totalTps);
        System.out.printf("latency avg: %.3f us%n", avgMicros);

        long[] all = new long[totalCount];
        int offset = 0;
        for (WorkerResult result : results) {
            System.arraycopy(result.latencies, 0, all, offset, result.count);
            offset += result.count;
        }
        Arrays.sort(all);
        System.out.printf("latency    : p50=%.1f us, p90=%.1f us, p99=%.1f us, max=%.1f us%n",
                percentileMicros(all, 50), percentileMicros(all, 90),
                percentileMicros(all, 99), all[all.length - 1] / 1000.0);
    }

    /** Computes the p-th latency percentile (microseconds); samples must be sorted ascending. */
    private static double percentileMicros(long[] sorted, int p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(sorted.length - 1, index));
        return sorted[index] / 1000.0;
    }

}
