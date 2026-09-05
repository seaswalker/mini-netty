package com.github.skywalker.mininetty.bootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.github.skywalker.mininetty.client.TestSupport;
import com.github.skywalker.mininetty.context.MessageProcessingContext;
import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.InBoundHandlerAdapter;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import com.github.skywalker.mininetty.manager.ManagerGroup;
import com.github.skywalker.mininetty.server.Server;

/**
 * Robustness edge-case tests for {@link Server}, exercised purely through its
 * public API and real sockets (no source changes, no white-box hooks).
 *
 * <p>These cases are deliberately <em>not</em> covered by the existing
 * {@link LifeCycleStateTest} / {@link ServerTest} matrix:</p>
 * <ol>
 *   <li>multiple servers sharing a single {@link ManagerGroup};</li>
 *   <li>{@code close()} while an accepted connection is still active (peer must
 *       observe the severing, the port must be released, and the manager group
 *       must remain reusable);</li>
 *   <li>binding to a port that is already in use (fast failure, safe close);</li>
 *   <li>a handler throwing per-connection (failure must stay isolated);</li>
 *   <li>connection churn (many connect/echo/close cycles must not wedge the server).</li>
 * </ol>
 *
 * @author skywalker
 */
public class ServerRobustnessTest {

    /**
     * Port allocator, kept in a range that never collides with {@link TestSupport}
     * or {@link LifeCycleStateTest}.
     */
    private static final AtomicInteger PORT = new AtomicInteger(28000);

    /** Selector threads for the manager groups created in this test class. */
    private static final int SELECTOR_COUNT = 1;
    /** Worker threads for the manager groups created in this test class. */
    private static final int WORKER_COUNT = 4;

    @Test
    public void multipleServersShareOneManagerGroup() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        int port1 = newPort();
        int port2 = newPort();
        Server server1 = echoServer(managerGroup, port1);
        Server server2 = echoServer(managerGroup, port2);
        try {
            server1.startAsync();
            server2.startAsync();
            awaitAccepting(port1);
            awaitAccepting(port2);

            assertEchoRoundTrip(port1);
            assertEchoRoundTrip(port2);

            // Closing one server must not affect the other, even though both are
            // served by the same selector thread.
            server1.close();
            awaitPortFree(port1);
            assertEchoRoundTrip(port2);
        } finally {
            server1.close();
            server2.close();
            managerGroup.close();
        }
    }

    @Test
    public void closeSeveringActiveConnectionFreesPortAndGroupStaysUsable() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        int port = newPort();
        Server server = echoServer(managerGroup, port);
        TestSupport.ClientConnection connection = null;
        try {
            server.startAsync();
            awaitAccepting(port);

            connection = TestSupport.connect(port);
            connection.writeLine("ping");
            Assert.assertEquals("ping", connection.readLine());

            // Shut the server down while a connection is still open.
            server.close();
            awaitPortFree(port);

            // The surviving connection must actually be severed: send data so the
            // selector wakes up on the (now cancelled) channel and closes it.
            connection.writeLine("still-there");
            Assert.assertNull("A connection that was open when the server closed "
                    + "must observe EOF afterwards.", connection.readLine());
            connection.close();
            connection = null;

            // The manager group itself is still healthy: a brand-new server can be
            // started on it and serve traffic.
            int reusePort = newPort();
            Server restarted = echoServer(managerGroup, reusePort);
            try {
                restarted.startAsync();
                awaitAccepting(reusePort);
                assertEchoRoundTrip(reusePort);
            } finally {
                restarted.close();
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void bindingToInUsePortFailsFastAndCloseIsSafe() throws Exception {
        int port = newPort();
        // Occupy the port with a plain blocking server socket.
        try (ServerSocket blocker = new ServerSocket(port)) {
            ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
            managerGroup.start();
            Server server = echoServer(managerGroup, port);
            try {
                server.startAsync();
                try {
                    server.startAsync().get(5, TimeUnit.SECONDS);
                    Assert.fail("Binding to an occupied port must fail, not succeed.");
                } catch (ExecutionException e) {
                    Assert.assertTrue("Expected an IOException from the bind failure but got: "
                            + e.getCause(), e.getCause() instanceof IOException);
                }
                // Closing a server whose bind failed must be a safe no-op.
                server.close();
                server.close();
            } finally {
                server.close();
                managerGroup.close();
            }
        }

        // Once the blocker is released the port is free again and a fresh server can
        // be started on it, proving neither the port nor the group was corrupted.
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        Server server = echoServer(managerGroup, port);
        try {
            server.startAsync();
            awaitAccepting(port);
            assertEchoRoundTrip(port);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void handlerFailureIsIsolatedToOneConnection() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        int port = newPort();
        Server server = faultInjectingEchoServer(managerGroup, port);
        TestSupport.ClientConnection victim = null;
        TestSupport.ClientConnection bystander = null;
        try {
            server.startAsync();
            awaitAccepting(port);

            // A bystander connection echoes normally before and while the victim blows up.
            bystander = TestSupport.connect(port);
            bystander.writeLine("bystander-1");
            Assert.assertEquals("bystander-1", bystander.readLine());

            victim = TestSupport.connect(port);
            victim.writeLine(FaultInjectingHandler.TRIGGER);
            // The victim connection must be closed by the framework, so we read EOF.
            Assert.assertNull("A connection whose handler threw must be closed by the "
                    + "framework, not left hanging.", victim.readLine());
            victim.close();
            victim = null;

            // The bystander is unaffected and the server keeps accepting new clients.
            bystander.writeLine("bystander-2");
            Assert.assertEquals("bystander-2", bystander.readLine());
            bystander.close();
            bystander = null;

            try (TestSupport.ClientConnection newcomer = TestSupport.connect(port)) {
                newcomer.writeLine("newcomer");
                Assert.assertEquals("newcomer", newcomer.readLine());
            }
        } finally {
            if (victim != null) {
                victim.close();
            }
            if (bystander != null) {
                bystander.close();
            }
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void connectionChurnLeavesServerAcceptingAndResponding() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        int port = newPort();
        Server server = echoServer(managerGroup, port);
        try {
            server.startAsync();
            awaitAccepting(port);

            final int churn = 30;
            for (int i = 0; i < churn; i++) {
                try (TestSupport.ClientConnection connection = TestSupport.connect(port)) {
                    String payload = "round-" + i;
                    connection.writeLine(payload);
                    Assert.assertEquals(payload, connection.readLine());
                }
            }

            // After the churn the server still accepts fresh connections and echoes.
            assertEchoRoundTrip(port);
            assertEchoRoundTrip(port);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int newPort() {
        return PORT.getAndIncrement();
    }

    private static ManagerGroup newManagerGroup() {
        return new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
    }

    private static Server echoServer(ManagerGroup managerGroup, int port) {
        return echoHandlersOn(new Server(managerGroup).bind(port));
    }

    private static Server faultInjectingEchoServer(ManagerGroup managerGroup, int port) {
        return new Server(managerGroup).bind(port).setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return new Handler[] {
                        new DelimiterBasedDecoder((byte) '\n'),
                        new StringDecoder(),
                        new FaultInjectingHandler(),
                        new ResponseHandler()
                };
            }
        });
    }

    private static Server echoHandlersOn(Server server) {
        return server.setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return TestSupport.echoLineHandlers();
            }
        });
    }

    private static void assertEchoRoundTrip(int port) throws IOException {
        try (TestSupport.ClientConnection connection = TestSupport.connect(port)) {
            connection.writeLine("ping");
            Assert.assertEquals("ping", connection.readLine());
        }
    }

    /**
     * Blocks until the server on the given port accepts a connection (or the deadline
     * elapses), so a just-started server can be used immediately.
     */
    private static void awaitAccepting(int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", port), 500);
                return;
            } catch (IOException e) {
                lastFailure = e;
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        Assert.fail("Server did not accept connections on port " + port + " within 5s."
                + (lastFailure == null ? "" : " Last failure: " + lastFailure));
    }

    /**
     * Blocks until the given port can be bound again, i.e. the closed server channel
     * has actually been released by the selector thread (cleanup is asynchronous).
     */
    private static void awaitPortFree(int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.bind(new InetSocketAddress("localhost", port));
                return;
            } catch (IOException e) {
                lastFailure = e;
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        Assert.fail("Port " + port + " was not released within 5s after close()."
                + (lastFailure == null ? "" : " Last bind failure: " + lastFailure));
    }

    /**
     * Injects a failure into a single connection: any message equal to
     * {@link #TRIGGER} makes this handler throw, which must tear down only the
     * connection that sent it.
     */
    private static class FaultInjectingHandler extends InBoundHandlerAdapter {

        private static final String TRIGGER = "boom";

        @Override
        public void channelRead(Object message, MessageProcessingContext context) {
            if (TRIGGER.equals(message)) {
                throw new IllegalStateException("Fault injection triggered by '" + TRIGGER + "'.");
            }
            context.channelRead(message);
        }

    }

}
