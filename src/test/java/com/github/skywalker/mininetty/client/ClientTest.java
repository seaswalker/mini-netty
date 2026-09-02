package com.github.skywalker.mininetty.client;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.SimpleInBoundHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.LengthFieldBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import org.junit.Assert;
import org.junit.Test;
import com.github.skywalker.mininetty.server.Server;
import com.github.skywalker.mininetty.util.DataUtils;

/**
 * Integration tests that act as clients (plain sockets) against a {@link Server}.
 *
 * <p>The idle detection {@code IdleDetectionDuplexHandler} is automatically added by
 * {@code HandlerChain} when the handler chain is built, so the idle-detection test
 * cases below do not need to add it to the chain explicitly.</p>
 *
 * @author skywalker
 */
public class ClientTest {

    /** Server idle-detection threshold (seconds); matches {@code defaultIdleTimeoutSeconds} in {@code ChannelHandlerContext}. */
    private static final int IDLE_TIMEOUT_SECONDS = 5;
    /** Heartbeat interval, must be shorter than {@link #IDLE_TIMEOUT_SECONDS}. */
    private static final long HEARTBEAT_INTERVAL_MILLIS = 1000;
    /** Number of clients connecting to one server simultaneously in the multi-client scenario. */
    private static final int CONCURRENT_CLIENT_COUNT = 4;
    /** Number of messages each client sends in the multi-client scenario. */
    private static final int MESSAGES_PER_CLIENT = 3;

    @Test
    public void testLengthFieldBasedDecoder() throws IOException, InterruptedException {
        // Handlers must be created per connection, never shared across connections
        try (TestSupport.TestServer server = TestSupport.startServer(
                () -> new Handler[] {
                        new LengthFieldBasedDecoder(0, 4),
                        new StringDecoder(),
                        new SimpleInBoundHandler()
                });
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            byte[] frame = new byte[35];
            System.arraycopy(DataUtils.int2Bytes(31), 0, frame, 0, 4);
            System.arraycopy("org.apache.commons.lang.builder".getBytes(), 0, frame, 4, 31);
            client.write(frame);
            client.write(frame);
            // readLine strips the trailing newline
            Assert.assertEquals("org.apache.commons.lang.builder", client.readLine());
            Assert.assertEquals("org.apache.commons.lang.builder", client.readLine());
        }
    }

    @Test
    public void testDelimiterBasedDecoder() throws IOException, InterruptedException {
        try (TestSupport.TestServer server = TestSupport.startServer(
                () -> new Handler[] {
                        new DelimiterBasedDecoder('a'),
                        new StringDecoder(),
                        new ResponseHandler()
                });
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.write("This isadoga".getBytes());
            Assert.assertEquals("This is", client.readLine());
            Assert.assertEquals("dog", client.readLine());
        }
    }

    /**
     * Idle detection: when no data is sent after the connection is established, the
     * server should actively close the connection once the idle threshold is exceeded.
     */
    @Test
    public void testConnectionClosedWhenIdle() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            assertClosedByServer(client);
        }
    }

    /**
     * Asserts that the server closed the connection: EOF means the connection has
     * been closed. Fails if it is still open after the idle threshold plus grace period.
     */
    private static void assertClosedByServer(TestSupport.ClientConnection client) throws IOException {
        try {
            Assert.assertEquals("Expected EOF since the connection has been closed by the server.", -1, client.read());
        } catch (SocketTimeoutException e) {
            Assert.fail("The server did not close the connection within "
                    + TestSupport.READ_TIMEOUT_SECONDS + "s after it was idle for more than "
                    + IDLE_TIMEOUT_SECONDS + "s.");
        }
    }

    /**
     * Idle detection: as long as the interaction interval stays below the idle
     * threshold, the connection should not be closed by the server.
     */
    @Test
    public void testConnectionKeptAliveWhenActive() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(IDLE_TIMEOUT_SECONDS + 2);
            int seq = 0;
            while (System.nanoTime() < deadline) {
                String message = "ping-" + (seq++);
                client.writeLine(message);
                // The server echoes the message back; being able to read a response
                // means the connection is still alive
                Assert.assertEquals(message, client.readLine());
                TimeUnit.MILLISECONDS.sleep(HEARTBEAT_INTERVAL_MILLIS);
            }
        }
    }

    /**
     * Multi-client functional test: several clients connect to the same server at the
     * same time and exchange request-responses independently, verifying that the server
     * can maintain multiple connections concurrently without data cross-talk.
     */
    @Test
    public void testMultipleClientsShareOneServer() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_CLIENT_COUNT);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < CONCURRENT_CLIENT_COUNT; i++) {
                    final int clientIndex = i;
                    futures.add(pool.submit(() -> {
                        try (TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
                            for (int j = 0; j < MESSAGES_PER_CLIENT; j++) {
                                String message = "client-" + clientIndex + "-msg-" + j;
                                client.writeLine(message);
                                // The echoed content must match what was sent this time,
                                // otherwise data cross-talk has occurred between connections
                                Assert.assertEquals(message, client.readLine());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException("Client-" + clientIndex + " failed to send/receive.", e);
                        }
                    }));
                }
                // Any client exception propagates through the Future and fails this test
                for (Future<?> future : futures) {
                    future.get();
                }
            } finally {
                pool.shutdownNow();
            }
        }
    }

}
