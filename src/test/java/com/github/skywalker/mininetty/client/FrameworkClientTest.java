package com.github.skywalker.mininetty.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.github.skywalker.mininetty.context.ChannelHandlerContext;
import com.github.skywalker.mininetty.context.MessageProcessingContext;
import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import org.junit.Assert;
import org.junit.Test;
import com.github.skywalker.mininetty.manager.ManagerGroup;

/**
 * Black-box functional tests for the framework {@link Client}.
 *
 * <p>{@code Client} is driven exclusively through its public API
 * ({@code setManagerGroup}/{@code handlers}/{@code startAsync}/{@code close}) and is
 * always pointed at a real echo server started via {@link TestSupport}; assertions are
 * made on the observable request/response flow and on the lifecycle events that reach
 * the handlers attached to the client connection. No implementation detail of
 * {@code Client} is inspected.</p>
 *
 * <p>Like every server channel, a framework client connection also carries the idle
 * detection added by the handler chain, so an idle client connection is expected to be
 * closed once the ~5s idle threshold is exceeded.</p>
 *
 * @author skywalker
 */
public class FrameworkClientTest {

    /** Upper bound for waiting on channelActive after a connect. */
    private static final int CONNECT_AWAIT_SECONDS = 5;
    /** Upper bound for waiting on a single echoed message. */
    private static final int MESSAGE_AWAIT_SECONDS = 5;
    /** Upper bound for the close to propagate to the peer / to complete the future. */
    private static final int CLOSE_AWAIT_SECONDS = 5;
    /** Idle threshold (seconds); matches the idle detection added by the handler chain. */
    private static final int IDLE_THRESHOLD_SECONDS = 5;
    /** Upper bound for the idle connection to be closed (threshold + grace). */
    private static final int IDLE_AWAIT_SECONDS = IDLE_THRESHOLD_SECONDS + 3;
    /** Number of framework clients in the multi-client scenario. */
    private static final int CONCURRENT_CLIENT_COUNT = 4;
    /** Number of messages each client sends in the multi-client scenario. */
    private static final int MESSAGES_PER_CLIENT = 3;
    /** Worker threads per client-side manager group. */
    private static final int CLIENT_WORKER_COUNT = 8;

    /**
     * The client connects to an echo server, the very first round trip succeeds and the
     * connection stays open afterwards.
     */
    @Test
    public void connectAndEchoRoundTrip() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ManagerGroup group = newClientGroup();
            ClientHarness harness = new ClientHarness(group, server.port);
            try {
                harness.connect();
                Assert.assertTrue("channelActive must fire after a successful connect.",
                        harness.probe.awaitActive(CONNECT_AWAIT_SECONDS));

                harness.probe.send("hello");
                harness.probe.expectLine("hello", MESSAGE_AWAIT_SECONDS);

                // No idle close must happen while the connection is being actively used.
                Assert.assertFalse("Connection must stay open while active.",
                        harness.probe.awaitInactive(1));
            } finally {
                harness.close();
                group.close();
            }
        }
    }

    /**
     * One connection is reused for several request/response exchanges, which proves the
     * client connection stays usable after the first message and preserves message order.
     */
    @Test
    public void sequentialMessagesOnOneConnection() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ManagerGroup group = newClientGroup();
            ClientHarness harness = new ClientHarness(group, server.port);
            try {
                harness.connect();
                Assert.assertTrue("channelActive must fire after a successful connect.",
                        harness.probe.awaitActive(CONNECT_AWAIT_SECONDS));

                for (int i = 0; i < MESSAGES_PER_CLIENT; i++) {
                    String message = "seq-" + i;
                    harness.probe.send(message);
                    harness.probe.expectLine(message, MESSAGE_AWAIT_SECONDS);
                }
            } finally {
                harness.close();
                group.close();
            }
        }
    }

    /**
     * Several framework clients, sharing one client-side manager group, talk to the same
     * echo server concurrently. Each client must receive exactly the echoes of its own
     * messages, i.e. there must be no cross-talk between the concurrent connections.
     */
    @Test
    public void concurrentFrameworkClientsShareOneServer() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ManagerGroup group = newClientGroup();
            List<ClientHarness> harnesses = new ArrayList<>();
            for (int i = 0; i < CONCURRENT_CLIENT_COUNT; i++) {
                harnesses.add(new ClientHarness(group, server.port));
            }
            ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_CLIENT_COUNT);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < CONCURRENT_CLIENT_COUNT; i++) {
                    final int clientIndex = i;
                    final ClientHarness harness = harnesses.get(i);
                    futures.add(pool.submit(() -> {
                        try {
                            harness.connect();
                            Assert.assertTrue("Client-" + clientIndex + " must become active.",
                                    harness.probe.awaitActive(CONNECT_AWAIT_SECONDS));
                            for (int j = 0; j < MESSAGES_PER_CLIENT; j++) {
                                String message = "client-" + clientIndex + "-msg-" + j;
                                harness.probe.send(message);
                                harness.probe.expectLine(message, MESSAGE_AWAIT_SECONDS);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("Client-" + clientIndex + " was interrupted.", e);
                        }
                    }));
                }
                for (Future<?> future : futures) {
                    future.get();
                }
            } finally {
                pool.shutdownNow();
                for (ClientHarness harness : harnesses) {
                    harness.close();
                }
                group.close();
            }
        }
    }

    /**
     * Closing the framework client must sever the TCP connection (the echo server
     * observes the close on its side) and complete the future returned by
     * {@code startAsync}.
     */
    @Test
    public void clientCloseIsObservedByPeerAndCompletesFuture() throws Exception {
        // Counts only connections that exchanged at least one message, so the readiness
        // probe connection of startServer (which never sends data) is ignored.
        CountDownLatch peerSawClose = new CountDownLatch(1);
        try (TestSupport.TestServer server = TestSupport.startServer(
                () -> new Handler[] {
                        new DelimiterBasedDecoder((byte) '\n'),
                        new StringDecoder(),
                        new EchoAndTrackInactiveHandler(peerSawClose)
                })) {
            ManagerGroup group = newClientGroup();
            ClientHarness harness = new ClientHarness(group, server.port);
            try {
                Future<?> future = harness.connect();
                Assert.assertTrue("channelActive must fire after a successful connect.",
                        harness.probe.awaitActive(CONNECT_AWAIT_SECONDS));
                harness.probe.send("bye");
                harness.probe.expectLine("bye", MESSAGE_AWAIT_SECONDS);

                harness.close();

                Assert.assertTrue("The server must observe the client-initiated close.",
                        peerSawClose.await(CLOSE_AWAIT_SECONDS, TimeUnit.SECONDS));
                assertFutureDone(future, "future must complete after close()");
            } finally {
                harness.close();
                group.close();
            }
        }
    }

    /**
     * A framework client connection that exchanges no data must be closed once the idle
     * threshold is exceeded (either end may close it first; the observable contract is
     * that the connection no longer survives prolonged silence).
     */
    @Test
    public void idleConnectionIsClosed() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(TestSupport::echoLineHandlers)) {
            ManagerGroup group = newClientGroup();
            ClientHarness harness = new ClientHarness(group, server.port);
            try {
                harness.connect();
                Assert.assertTrue("channelActive must fire after a successful connect.",
                        harness.probe.awaitActive(CONNECT_AWAIT_SECONDS));

                // No traffic at all: the connection must not stay open past the idle window.
                Assert.assertTrue("An idle connection must be closed.",
                        harness.probe.awaitInactive(IDLE_AWAIT_SECONDS));
            } finally {
                harness.close();
                group.close();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ManagerGroup newClientGroup() {
        ManagerGroup group = new ManagerGroup(1, CLIENT_WORKER_COUNT);
        group.start();
        return group;
    }

    private static void assertFutureDone(Future<?> future, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLOSE_AWAIT_SECONDS);
        while (!future.isDone() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        Assert.assertTrue(message, future.isDone());
    }

    /**
     * A framework client connection to the given port, together with the probe handler
     * observing its lifecycle. Every harness owns a fresh set of handler instances, so
     * no state is shared between harnesses.
     */
    private static final class ClientHarness implements AutoCloseable {

        private final Probe probe = new Probe();
        private final Client client;

        ClientHarness(ManagerGroup group, int port) {
            this.client = new Client("localhost", port)
                    .setManagerGroup(group)
                    .handlers(
                            new DelimiterBasedDecoder((byte) '\n'),
                            new StringDecoder(),
                            probe
                    );
        }

        Future<?> connect() {
            return client.startAsync();
        }

        @Override
        public void close() {
            client.close();
        }

    }

    /**
     * Client-side handler that records the decoded messages and the channel lifecycle
     * events, and can send messages back through the captured channel. It forwards the
     * events down the chain so it behaves like a plain last handler.
     */
    private static final class Probe extends InBoundHandlerAdapter {

        private final BlockingQueue<String> received = new LinkedBlockingQueue<>();
        private final CountDownLatch activeLatch = new CountDownLatch(1);
        private final CountDownLatch inactiveLatch = new CountDownLatch(1);
        private volatile ChannelHandlerContext channel;

        @Override
        public void channelActive(MessageProcessingContext context) {
            this.channel = context.channel();
            activeLatch.countDown();
        }

        @Override
        public void channelRead(Object message, MessageProcessingContext context) {
            received.offer(String.valueOf(message));
        }

        @Override
        public void channelInActive(MessageProcessingContext context) {
            inactiveLatch.countDown();
        }

        boolean awaitActive(int seconds) throws InterruptedException {
            return activeLatch.await(seconds, TimeUnit.SECONDS);
        }

        boolean awaitInactive(int seconds) throws InterruptedException {
            return inactiveLatch.await(seconds, TimeUnit.SECONDS);
        }

        /** Sends a message (the newline delimiter is appended for the echo server). */
        void send(String message) {
            channel.writeAndFlush(message + "\n");
        }

        /** Asserts the next received message equals {@code expected}, within the timeout. */
        void expectLine(String expected, int seconds) throws InterruptedException {
            String actual = received.poll(seconds, TimeUnit.SECONDS);
            Assert.assertNotNull("Expected a message but nothing arrived.", actual);
            Assert.assertEquals(expected, actual);
        }

    }

    /**
     * Server-side handler that echoes a message back and reports when its connection is
     * closed, but only if the connection actually exchanged a message (this way the
     * readiness probe connection of {@link TestSupport#startServer} is ignored).
     */
    private static final class EchoAndTrackInactiveHandler extends InBoundHandlerAdapter {

        private final CountDownLatch closedByPeer;
        private boolean talked = false;

        EchoAndTrackInactiveHandler(CountDownLatch closedByPeer) {
            this.closedByPeer = closedByPeer;
        }

        @Override
        public void channelRead(Object message, MessageProcessingContext context) {
            talked = true;
            context.channel().writeAndFlush(String.valueOf(message) + "\n");
        }

        @Override
        public void channelInActive(MessageProcessingContext context) {
            if (talked) {
                closedByPeer.countDown();
            }
            context.channelInactive();
        }

    }

}
