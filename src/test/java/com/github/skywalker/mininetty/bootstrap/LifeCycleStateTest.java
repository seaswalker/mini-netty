package com.github.skywalker.mininetty.bootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.github.skywalker.mininetty.client.Client;
import com.github.skywalker.mininetty.client.TestSupport;
import com.github.skywalker.mininetty.exception.MiniNettyIllegalStateException;
import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.manager.ManagerGroup;
import com.github.skywalker.mininetty.server.Server;

/**
 * Lifecycle / state-machine "torture" tests for {@link Server}, {@link Client} and
 * {@link ManagerGroup}.
 *
 * <p>The lifecycle is one-shot: components transition {@code INIT -> STARTED -> CLOSED}
 * and never go backwards. The tests below pin down the exact legal and illegal
 * transitions, idempotency of repeated calls, validation ordering of
 * {@code startAsync()} and, most importantly, whether resources (threads, ports) are
 * actually released again after {@code close()}.</p>
 *
 * @author skywalker
 */
public class LifeCycleStateTest {

    /** Port allocator, kept in a range that never collides with {@link TestSupport}. */
    private static final AtomicInteger PORT = new AtomicInteger(19090);

    @Test
    public void bindRejectsPortBelowOne() {
        Server server = new Server(neverStartedGroup());
        assertMiniNettyStateException("The port must be greater than 0.", () -> server.bind(0));
        assertMiniNettyStateException("The port must be greater than 0.", () -> server.bind(-1));
    }

    @Test
    public void managerGroupRejectsNonPositiveCounts() {
        assertMiniNettyStateException("Selector count and worker count must be greater than 0",
                () -> new ManagerGroup(0, 2));
        assertMiniNettyStateException("Selector count and worker count must be greater than 0",
                () -> new ManagerGroup(1, 0));
        assertMiniNettyStateException("Selector count and worker count must be greater than 0",
                () -> new ManagerGroup(-2, -2));
    }

    @Test
    public void managerGroupSelectorAccessRequiresStartedState() {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        // Not started yet.
        assertMiniNettyStateException("Unstarted", managerGroup::getSelectorManager);
        managerGroup.close();
        // Closed-from-INIT is still not "started".
        assertMiniNettyStateException("Unstarted", managerGroup::getSelectorManager);

        ManagerGroup started = new ManagerGroup(1, 2);
        try {
            started.start();
            started.start(); // idempotent
            Assert.assertNotNull(started.getSelectorManager().chooseOne());
            started.close();
            started.close(); // idempotent
            assertMiniNettyStateException("Unstarted", started::getSelectorManager);
        } finally {
            started.close();
        }
    }

    @Test
    public void serverStartRequiresStartedManagerGroup() {
        // ManagerGroup was never started, so registration must be refused.
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        Server server = echoServer(managerGroup, newPort());
        try {
            assertMiniNettyStateException("Unstarted", server::startAsync);
        } finally {
            server.close();
        }
    }

    @Test
    public void serverStartRequiresHandlers() {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        Server server = new Server(managerGroup).bind(newPort());
        try {
            assertMiniNettyStateException("No handlers specified", server::startAsync);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void serverStartRequiresBoundPort() {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        Server server = echoHandlersOn(new Server(managerGroup));
        try {
            assertMiniNettyStateException("No port bound?", server::startAsync);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void serverCanBeRestartedAfterConfigurationFailure() throws Exception {
        // A configuration failure (missing handlers) surfaces on startAsync. A robust
        // lifecycle would roll the state back so a corrected startAsync can recover.
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        int port = newPort();
        Server server = new Server(managerGroup).bind(port);
        try {
            assertMiniNettyStateException("No handlers specified", server::startAsync);
            // Fix the configuration and retry.
            server.setHandlers(new HandlerInitializer() {
                @Override
                public Handler[] init() {
                    return TestSupport.echoLineHandlers();
                }
            });
            server.startAsync();
            awaitAccepting(port);
            assertEchoRoundTrip(port);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void doubleStartAsyncIsIdempotentAndCloseReleasesPort() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        int port = newPort();
        Server server = echoServer(managerGroup, port);
        try {
            server.startAsync();
            server.startAsync(); // second call must not double-register/bind the same port
            awaitAccepting(port);
            assertEchoRoundTrip(port);

            server.close();
            server.close(); // close must be idempotent
            awaitPortFree(port);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void serverCannotBeRestartedAfterClose() throws Exception {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        int port = newPort();
        Server server = echoServer(managerGroup, port);
        try {
            server.startAsync();
            awaitAccepting(port);
            assertEchoRoundTrip(port);

            server.close();
            awaitPortFree(port);

            // Lifecycle is one-shot: starting a closed component is a silent no-op
            // (must not throw, and must not resurrect the bound port).
            server.startAsync();
            assertPortNotAccepting(port);
        } finally {
            server.close();
            managerGroup.close();
        }
    }

    @Test
    public void portIsReusableAcrossCleanServerRestarts() throws Exception {
        int port = newPort();

        ManagerGroup first = new ManagerGroup(1, 2);
        first.start();
        Server server1 = echoServer(first, port);
        try {
            server1.startAsync();
            awaitAccepting(port);
            assertEchoRoundTrip(port);
            server1.close();
            awaitPortFree(port);
        } finally {
            server1.close();
            first.close();
        }

        ManagerGroup second = new ManagerGroup(1, 2);
        second.start();
        Server server2 = echoServer(second, port);
        try {
            server2.startAsync();
            awaitAccepting(port);
            assertEchoRoundTrip(port);
        } finally {
            server2.close();
            second.close();
        }
    }

    @Test
    public void clientValidatesConfigurationBeforeStart() {
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        try {
            Client client = new Client("localhost", newPort());
            assertIllegalArgument("ManagerGroup not set", client::startAsync);
            client.close(); // closing an unstarted client must be safe and one-shot
            client.close();

            Client withoutHandlers = new Client("localhost", newPort())
                    .setManagerGroup(managerGroup);
            assertIllegalArgument("Handlers not set", withoutHandlers::startAsync);
            withoutHandlers.close();
        } finally {
            managerGroup.close();
        }
    }

    @Test
    public void clientConnectToDeadPortFailsInsteadOfHanging() throws Exception {
        int deadPort = unusedPort();
        ManagerGroup managerGroup = new ManagerGroup(1, 2);
        managerGroup.start();
        Client client = new Client("localhost", deadPort)
                .setManagerGroup(managerGroup)
                .handlers(new HandlerInitializer() {
                    @Override
                    public Handler[] init() {
                        return TestSupport.echoLineHandlers();
                    }
                });
        try {
            Future<?> future = client.startAsync();
            try {
                future.get(5, TimeUnit.SECONDS);
                Assert.fail("Connecting to a dead port must fail, not succeed.");
            } catch (ExecutionException e) {
                Assert.assertTrue("Unexpected failure cause: " + e.getCause(),
                        e.getCause() instanceof IOException);
            }
        } finally {
            client.close();
            managerGroup.close();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int newPort() {
        return PORT.getAndIncrement();
    }

    /** Creates a manager group that is never started and never needs closing. */
    private static ManagerGroup neverStartedGroup() {
        return new ManagerGroup(1, 2);
    }

    private static Server echoServer(ManagerGroup managerGroup, int port) {
        return echoHandlersOn(new Server(managerGroup).bind(port));
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
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", port), 500);
                return;
            } catch (IOException e) {
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        Assert.fail("Server did not accept connections on port " + port + " within 3s.");
    }

    /**
     * Asserts that nothing is listening on the given port.
     */
    private static void assertPortNotAccepting(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", port), 200);
            Assert.fail("Expected no listener on port " + port + " but a connection succeeded.");
        } catch (IOException ignored) {
            // Expected: connection refused.
        }
    }

    /**
     * Blocks until the given port can be bound again, i.e. the closed server channel
     * has actually been released by the selector thread (cleanup is asynchronous).
     */
    private static void awaitPortFree(int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
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
        Assert.fail("Port " + port + " was not released within 3s after close()."
                + (lastFailure == null ? "" : " Last bind failure: " + lastFailure));
    }

    /** Finds a port that is currently unused. */
    private static int unusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static <T extends Throwable> T assertThrows(Class<T> expected, Runnable action) {
        try {
            action.run();
        } catch (Throwable t) {
            if (expected.isInstance(t)) {
                return expected.cast(t);
            }
            throw new AssertionError("Expected " + expected.getName() + " but got "
                    + t.getClass().getName() + ": " + t.getMessage(), t);
        }
        throw new AssertionError("Expected " + expected.getName() + " but nothing was thrown.");
    }

    private static void assertMiniNettyStateException(String message, Runnable action) {
        MiniNettyIllegalStateException e =
                assertThrows(MiniNettyIllegalStateException.class, action);
        Assert.assertEquals(message, e.getMessage());
    }

    private static void assertIllegalArgument(String message, Runnable action) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, action);
        Assert.assertEquals(message, e.getMessage());
    }

}
