package com.github.skywalker.mininetty.bootstrap;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.github.skywalker.mininetty.handler.Handler;
import com.github.skywalker.mininetty.handler.HandlerInitializer;
import com.github.skywalker.mininetty.handler.ResponseHandler;
import com.github.skywalker.mininetty.handler.SimpleInBoundHandler;
import com.github.skywalker.mininetty.handler.codec.decoder.DelimiterBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.LengthFieldBasedDecoder;
import com.github.skywalker.mininetty.handler.codec.decoder.StringDecoder;
import com.github.skywalker.mininetty.handler.codec.encoder.StringEncoder;
import com.github.skywalker.mininetty.server.Server;

/**
 * Basic server smoke tests.
 * 
 * @author skywalker
 *
 */
public class ServerTest {

    private static int PORT = 8081;

    @Test
    public void lengthFieldBasedDecoder() throws InterruptedException {
        Server server = new Server();
        server.bind(PORT++).setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return new Handler[] {new LengthFieldBasedDecoder(0, 4), new StringDecoder(), new SimpleInBoundHandler()};
            }
        }).start();

        TimeUnit.SECONDS.sleep(2);

        server.close();
    }

    @Test
    public void delimiterBasedDecoder() throws InterruptedException {
        Server server = new Server();
        server.bind(PORT++).setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return new Handler[] {new DelimiterBasedDecoder('a'), new StringDecoder(), new SimpleInBoundHandler()};
            }
        }).start();

        TimeUnit.SECONDS.sleep(2);

        server.close();
    }

    @Test
    public void response() throws InterruptedException {
        Server server = new Server();
        server.bind(PORT++).setHandlers(new StringDecoder(), new ResponseHandler(), new StringEncoder()).start();

        TimeUnit.SECONDS.sleep(2);

        server.close();
    }

}
