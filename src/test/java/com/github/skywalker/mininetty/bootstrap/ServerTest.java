package com.github.skywalker.mininetty.bootstrap;

import org.junit.Assert;
import org.junit.Test;

import com.github.skywalker.mininetty.client.TestSupport;
import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.SimpleInBoundHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.LengthFieldBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.LineBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import com.github.skywalker.mininetty.handler.codec.encoder.StringEncoder;
import com.github.skywalker.mininetty.util.DataUtils;

/**
 * Server smoke tests: boots a real {@link com.github.skywalker.mininetty.server.Server}
 * equipped with a given handler chain and verifies a request/response round-trip, which
 * also exercises server startup and shutdown.
 *
 * @author skywalker
 */
public class ServerTest {

    @Test
    public void lengthFieldBasedDecoder() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(() -> new Handler[] {
                        new LengthFieldBasedDecoder(0, 4),
                        new StringDecoder(),
                        new SimpleInBoundHandler()});
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            // 4-byte length-prefixed payload; SimpleInBoundHandler echoes everything after the prefix
            byte[] frame = new byte[35];
            System.arraycopy(DataUtils.int2Bytes(31), 0, frame, 0, 4);
            System.arraycopy("org.apache.commons.lang.builder".getBytes(), 0, frame, 4, 31);
            client.write(frame);
            Assert.assertEquals("org.apache.commons.lang.builder", client.readLine());
        }
    }

    @Test
    public void delimiterBasedDecoder() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(() -> new Handler[] {
                        new DelimiterBasedDecoder((byte) 'a'),
                        new StringDecoder(),
                        new ResponseHandler()});
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.write("This isadoga".getBytes());
            Assert.assertEquals("This is", client.readLine());
            Assert.assertEquals("dog", client.readLine());
        }
    }

    @Test
    public void response() throws Exception {
        try (TestSupport.TestServer server = TestSupport.startServer(() -> new Handler[] {
                        new LineBasedDecoder(),
                        new StringDecoder(),
                        new ResponseHandler(),
                        new StringEncoder()});
             TestSupport.ClientConnection client = TestSupport.connect(server.port)) {
            client.writeLine("ping");
            Assert.assertEquals("ping", client.readLine());
        }
    }

}
