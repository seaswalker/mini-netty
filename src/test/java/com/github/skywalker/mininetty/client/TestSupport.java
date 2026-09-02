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
    /** How long to wait for the server to become ready after startup. */
    private static final long SERVER_READY_WAIT_SECONDS = 2;
    /** Test port allocator, so each test case/server uses a distinct port. */
    private static final AtomicInteger PORT = new AtomicInteger(8081);

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
                new DelimiterBasedDecoder('\n'),
                new StringDecoder(),
                new ResponseHandler()
        };
    }

    /**
     * Starts a server, returns its bound port and waits until it is ready.
     *
     * @param handlerFactory a handler factory; it is invoked every time a client
     *                       connection is established (i.e. {@link HandlerInitializer#init()}
     *                       is called) to produce a dedicated set of handler instances
     *                       per connection. Never reuse pre-created instances, otherwise
     *                       stateful decoders would share state between connections.
     */
    public static TestServer startServer(final Supplier<Handler[]> handlerFactory) throws InterruptedException {
        int port = PORT.getAndIncrement();
        Server server = new Server();
        server.bind(port).setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return handlerFactory.get();
            }
        }).start();
        log.info("Server started, listening on port {}.", port);
        TimeUnit.SECONDS.sleep(SERVER_READY_WAIT_SECONDS);
        return new TestServer(server, port);
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
     * A wrapper around a server and its port, used for test cleanup.
     */
    public static class TestServer implements AutoCloseable {

        private final Server server;
        final int port;

        TestServer(Server server, int port) {
            this.server = server;
            this.port = port;
        }

        @Override
        public void close() {
            server.close();
        }

    }

}
