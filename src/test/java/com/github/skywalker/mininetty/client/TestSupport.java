package com.github.skywalker.mininetty.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.skywalker.mininetty.manager.ManagerGroup;
import com.github.skywalker.mininetty.server.Server;

/**
 * Shared infrastructure for the client integration/performance tests: starts a
 * server and opens plain socket connections to it.
 *
 * <p>Handlers are wrapped in a {@link HandlerInitializer} so that every new
 * connection gets its own fresh {@link Handler} instances (especially the stateful
 * decoders), avoiding state cross-talk between different connections.</p>
 *
 * @author skywalker
 */
public final class TestSupport {

    private static final Logger log = LoggerFactory.getLogger(TestSupport.class);

    /**
     * Per-read timeout (seconds) that prevents a test case from blocking forever
     * when the server misbehaves.
     * <p>The idle detection closes idle connections after roughly 5 seconds, so
     * this timeout leaves some headroom on top of that.</p>
     */
    public static final int READ_TIMEOUT_SECONDS = 10;
    /** Connection timeout. */
    private static final int CONNECT_TIMEOUT_MILLIS = 1000;
    /** How long the server is given to start accepting connections. */
    private static final long SERVER_READY_WAIT_SECONDS = 2;
    /** Test port allocator, so each test case/server uses a distinct port. */
    private static final AtomicInteger PORT = new AtomicInteger(8081);
    /** Selector threads owned by each test manager group. */
    private static final int SELECTOR_COUNT = 1;
    /**
     * Worker threads owned by each test manager group: large enough for the
     * concurrent-client scenarios to get distinct workers, small enough to keep
     * the per-test thread footprint low.
     */
    private static final int WORKER_COUNT = 4;

    private TestSupport() {
    }

    /**
     * A newline-delimited request-response handler chain; every invocation returns
     * a fresh set of handler instances.
     *
     * <p>Use with {@link #startServer(Supplier)} via a method reference, e.g.
     * {@code startServer(TestSupport::echoLineHandlers)}, so each connection holds
     * its own handlers.</p>
     */
    public static Handler[] echoLineHandlers() {
        return new Handler[] {
                new DelimiterBasedDecoder((byte) '\n'),
                new StringDecoder(),
                new ResponseHandler()
        };
    }

    /**
     * Starts a server, returns its bound port and waits until it accepts connections.
     *
     * <p>After the refactor the thread pools live in an external {@link ManagerGroup}
     * that must be started before the {@link Server} can register its channel, so this
     * method creates and starts a dedicated group per server and lets {@link TestServer}
     * shut it down again. {@link Server#startAsync()} is used instead of
     * {@link Server#start()}: the latter blocks until the server is closed.</p>
     *
     * @param handlerFactory a handler factory; it is invoked every time a client
     *                       connection is established (i.e. {@link HandlerInitializer#init()}
     *                       is called) to produce a dedicated set of handler instances
     *                       per connection. Never reuse pre-created instances, otherwise
     *                       stateful decoders would share state between connections.
     */
    public static TestServer startServer(final Supplier<Handler[]> handlerFactory) throws InterruptedException {
        int port = PORT.getAndIncrement();
        ManagerGroup managerGroup = new ManagerGroup(SELECTOR_COUNT, WORKER_COUNT);
        managerGroup.start();
        Server server = new Server(managerGroup);
        server.bind(port).setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return handlerFactory.get();
            }
        }).startAsync();
        awaitAccepting(port);
        log.info("Server started, listening on port {}.", port);
        return new TestServer(server, managerGroup, port);
    }

    /**
     * Blocks until the server on the given port accepts a connection (or the readiness
     * deadline elapses), so tests can connect immediately after {@link #startServer}.
     */
    private static void awaitAccepting(int port) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SERVER_READY_WAIT_SECONDS);
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", port), CONNECT_TIMEOUT_MILLIS);
                return;
            } catch (IOException e) {
                lastFailure = e;
                TimeUnit.MILLISECONDS.sleep(50);
            }
        }
        throw new IllegalStateException("Server did not accept connections on port " + port
                + " within " + SERVER_READY_WAIT_SECONDS + "s.", lastFailure);
    }

    /**
     * Opens a client connection to the given port.
     */
    public static ClientConnection connect(int port) throws IOException {
        Socket socket = new Socket();
        log.info("Connecting to localhost:{}.", port);
        socket.connect(new InetSocketAddress("localhost", port), CONNECT_TIMEOUT_MILLIS);
        socket.setSoTimeout((int) TimeUnit.SECONDS.toMillis(READ_TIMEOUT_SECONDS));
        return new ClientConnection(socket);
    }

    /**
     * A wrapper around a single client connection that handles socket reads and writes.
     */
    public static class ClientConnection implements AutoCloseable {

        private final Socket socket;
        private final BufferedOutputStream bos;
        private final BufferedReader br;

        ClientConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.bos = new BufferedOutputStream(socket.getOutputStream());
            this.br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }

        public void write(byte[] data) throws IOException {
            bos.write(data);
            bos.flush();
        }

        public void writeLine(String line) throws IOException {
            write((line + "\n").getBytes());
        }

        /**
         * Reads one line (without the trailing newline), or returns null when the
         * server closes the connection.
         */
        public String readLine() throws IOException {
            return br.readLine();
        }

        /**
         * Reads one byte, or returns -1 on EOF (server closed the connection).
         */
        public int read() throws IOException {
            return socket.getInputStream().read();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }

    }

    /**
     * A wrapper around a server, its manager group and its port, used for test cleanup.
     */
    public static class TestServer implements AutoCloseable {

        private final Server server;
        private final ManagerGroup managerGroup;
        /** The port the server is listening on. */
        public final int port;

        TestServer(Server server, ManagerGroup managerGroup, int port) {
            this.server = server;
            this.managerGroup = managerGroup;
            this.port = port;
        }

        @Override
        public void close() {
            // Stop the server first, then tear down the selector/worker threads it used.
            server.close();
            managerGroup.close();
        }

    }

}
