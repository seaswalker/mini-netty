package com.github.skywalker.mininetty.client;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.LineBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import com.github.skywalker.mininetty.server.Server;

/**
 * Integration tests that boot a real {@link Server} equipped with the
 * {@link LineBasedDecoder} and exchange newline-framed text over a plain socket.
 *
 * <p>Every connection gets a fresh {@link LineBasedDecoder} (via
 * {@link com.github.skywalker.mininetty.handler.HandlerInitializer}), so partial
 * lines buffered by one connection never leak into another one.</p>
 *
 * @author skywalker
 */
public class LineBasedDecoderIntegrationTest {

    @Test
    public void echoLinesTerminatedByLfAndCrLf() throws IOException, InterruptedException {
        try (TestSupport.TestServer server = TestSupport.startServer(LineBasedDecoderIntegrationTest::echoHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.write("first\nsecond\r\n".getBytes());
            Assert.assertEquals("first", client.readLine());
            Assert.assertEquals("second", client.readLine());
        }
    }

    @Test
    public void emptyLinesAreEchoedToo() throws IOException, InterruptedException {
        try (TestSupport.TestServer server = TestSupport.startServer(LineBasedDecoderIntegrationTest::echoHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.write("a\n\nb\r\n\r\nc\n".getBytes());
            Assert.assertEquals("a", client.readLine());
            Assert.assertEquals("", client.readLine());
            Assert.assertEquals("b", client.readLine());
            Assert.assertEquals("", client.readLine());
            Assert.assertEquals("c", client.readLine());
        }
    }

    @Test
    public void lineSplitAcrossSocketWritesIsReassembled() throws IOException, InterruptedException {
        try (TestSupport.TestServer server = TestSupport.startServer(LineBasedDecoderIntegrationTest::echoHandlers);
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.write("Hel".getBytes());
            Thread.sleep(100);
            client.write("lo ".getBytes());
            Thread.sleep(100);
            client.write("World\n".getBytes());
            Assert.assertEquals("Hello World", client.readLine());
        }
    }

    /** Fresh per-connection handlers: line framing -> String -> echo. */
    private static Handler[] echoHandlers() {
        return new Handler[] {
                new LineBasedDecoder(),
                new StringDecoder(),
                new ResponseHandler()
        };
    }

}
