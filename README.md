[![Build Status](https://travis-ci.org/seaswalker/netty-wheel.svg?branch=master)](https://travis-ci.org/seaswalker/netty-wheel)
[![codecov](https://codecov.io/gh/seaswalker/netty-wheel/branch/master/graph/badge.svg)](https://codecov.io/gh/seaswalker/netty-wheel)

# netty-wheel

A lightweight, educational re-implementation of a Netty-style network framework built purely on
Java NIO (`java.nio`). It demonstrates how an event-driven, non-blocking server and client work
under the hood - selectors, worker threads, handler pipelines and channel contexts - with no
runtime dependency on Netty itself. The server and every client share one `ManagerGroup`, so a
whole application runs on a small, fixed set of selector and worker threads.

> Java 8 required. This is a study project, not a production framework.

> A note on authorship: all the **source code** in `src/main` is hand-written. AI (GitHub
> Copilot) only helped write the tests and this documentation. 😄

## Features

### Channel events and the handler pipeline

- Inbound / outbound / duplex handler abstractions: `Handler`, `InBoundHandler`,
  `OutBoundHandler`, plus ready-to-extend adapters (`InBoundHandlerAdapter`,
  `OutBoundHandlerAdapter`, `DuplexHandlerAdapter`).
- Lifecycle and I/O events: `channelActive`, `channelInActive`, `channelRead`, `channelWrite`,
  `channelExceptionCaught`.
- `HandlerChain`: inbound events flow head to tail, outbound events flow tail back to head.
- `HandlerInitializer`: the recommended way to register server-side user handlers. Its `init()`
  method is invoked every time a connection is accepted, and the returned handlers are spliced
  into that connection's private chain (see Important notes). The `Client` side has no
  `HandlerInitializer`: you pass its handler chain directly through `handlers(...)`.

### Built-in handlers (installed automatically per connection)

Every connection - whether accepted by a `Server` or opened by a `Client` - gets a fresh
`HandlerChain` that already contains the following built-in handlers, so you do not - and should
not - register them yourself:

| Handler | Type | Responsibility |
| --- | --- | --- |
| `LoggingOutboundHandler` | outbound | Logs exceptions raised for a client channel. |
| `ChannelCleanupInboundHandler` | inbound | Closes the socket channel when the connection becomes inactive. |
| `IdleDetectionDuplexHandler` | duplex | Refreshes the per-channel `lastActiveTime` on every read and write. |
| `DefaultOutBoundHandler` | outbound | Terminal writer: converts `String` / `byte[]` / `ByteBuffer`, queues pending writes, handles partial writes and write back-pressure. |

### Codecs

Inbound decoders (each forwards decoded messages to the next handler):

- `LengthFieldBasedDecoder` - splits a stream by a length field
  (`offset`, `length`, optional `ByteOrder` and `maxLength`; little-endian by default).
  Handles both sticky packets and half packets.
- `DelimiterBasedDecoder` - splits by a single **ASCII** delimiter (the delimiter is stripped).
- `LineBasedDecoder` - a dedicated newline-framing decoder that strips either `\n` or `\r\n`
  (an optional `maxFrameLength` guards against oversized lines). It buffers partial lines across
  reads and emits every frame as a `byte[]`.
- `StringDecoder` - converts `byte[]` / heap `ByteBuffer` into `String`
  (UTF-8 by default, charset configurable).

Outbound encoder:

- `StringEncoder` - converts a `String` into a `ByteBuffer` on the write path.

### Concurrency model

- **`ManagerGroup` is the shared unit of thread resources.** It bundles a `SelectorManager` (N
  selector threads) and a `WorkerManager` (M worker threads). One group can back any number of
  `Server`s and `Client`s at the same time: every connection - whether accepted by a `Server` or
  opened by a `Client` - runs on the group's threads, never on per-connection threads. Defaults
  are 1 selector and one worker per CPU core; `new ManagerGroup(selectorCount, workerCount)`
  overrides them.
- **Selector threads** (`QueuedSelector`) accept new connections and dispatch readable / writable
  events.
- **Worker threads** are assigned round-robin per connection and **bind** the connection to
  themselves, so all events of one connection are executed serially on a single worker thread.
- Each connection owns an independent `ChannelHandlerContext`, `HandlerChain`, decoder state and
  outbound write queue. State is never shared between clients.

### Idle detection

- Every connection is checked periodically - whether accepted by a `Server` or opened by a
  `Client`. When no read or write happens within the idle timeout (default **5 seconds**), the
  framework logs a message and actively closes the channel.
- Any read or write refreshes `lastActiveTime` via `IdleDetectionDuplexHandler`, so heartbeats or
  regular traffic keep the connection alive.

### Write path and back-pressure

`DefaultOutBoundHandler` sits at the end of every outbound chain and:

- accepts `String`, `byte[]` or `ByteBuffer` messages,
- queues whatever could not be written completely, registers `OP_WRITE` and keeps draining the
  queue when the socket send buffer becomes writable again,
- toggles the channel writability when the pending queue exceeds the threshold (~20 KB).

## Thread model

All I/O threads live inside one `ManagerGroup`. A group is created per application, started once
and shared by the `Server` and every `Client`: accepted connections and client channels alike
bind onto the group's threads, so the total thread count never grows with the number of
connections. A connection flows through the selector tier and is then permanently bound to one
worker, so its handler chain always executes on the same thread:

```
┌──────────────────────────────────────────────────────────────────┐
│ ManagerGroup  (shared by the Server and every Client)            │
│                                                                  │
│  SelectorManager    (queued-selector-*)                          │
│  · N QueuedSelector threads, each owns a java.nio.Selector and   │
│    a per-thread task queue (interest-op changes never race the   │
│    select loop)                                                  │
│  · registers channels and accepts connections; select() dispatch:│
│      OP_READ    -> read the socket, hand bytes to the            │
│                    connection's bound worker                     │
│      OP_WRITE   -> ask the bound worker to drain writes          │
│                            │ bind connection (round-robin)       │
│                            ▼                                    │
│  WorkerManager    (mini-netty-worker-*)                          │
│  · M worker threads; 1 connection is permanently bound to        │
│    1 worker -> events run serially, no locks needed              │
│  · executes the per-connection chain:                            │
│      decoder -> user handler -> encoder                          │
│  · drains pending writes, applies back-pressure                  │
│  · scheduled tasks: idle detection (5 s)                         │
└──────────────────────────────────────────────────────────────────┘
```

* **`ManagerGroup`** - the shared pool of selector and worker threads. Start it once and reuse
  it for the server and all clients; `group.close()` stops the whole group. Defaults are 1
  selector and one worker per CPU core; `new ManagerGroup(selectorCount, workerCount)` overrides
  them.
* **QueuedSelector** - performs the real NIO `select()`. Each thread owns its own
  `java.nio.Selector` and a task queue, so interest-op changes never race the select loop. It
  registers channels and accepts new connections itself - there is no separate acceptor pool. On
  `OP_READ` it reads the socket and hands the bytes to the connection's bound worker; on
  `OP_WRITE` it asks that worker to drain the pending write queue.
* **Worker** - the connection's event loop. Because a connection is permanently bound to exactly
  one worker, all of its channel events run serially on that thread and need no locks. Scheduled
  tasks (e.g. idle detection) run here too.

## Getting started

### 1. Boot a server

```java
// One ManagerGroup holds the shared I/O threads; defaults: 1 selector + one worker per CPU core.
ManagerGroup group = new ManagerGroup();
group.start();

Server server = new Server(group)
        .bind(8080)
        .setHandlers(new HandlerInitializer() {
            @Override
            public Handler[] init() {
                return new Handler[] {
                    new DelimiterBasedDecoder((byte) '\n'),  // frames end with '\n'
                    new StringDecoder(),
                    new EchoHandler()
                };
            }
        });
server.startAsync();   // returns as soon as the server is listening
```

A `Server` is always built on a started `ManagerGroup`. `startAsync()` registers the listening
channel and returns immediately; `start()` instead blocks the calling thread until the server is
closed. Call `server.close()` to stop it.

### 2. Write handlers

Server side - reply to each decoded request:

```java
public class EchoHandler extends InBoundHandlerAdapter {
    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        // message has already been decoded to a String by the previous handler
        String request = (String) message;
        context.channel().writeAndFlush("echo: " + request + "\n");
    }
}
```

Client side - hold the connection and react to replies:

```java
public class ClientHandler extends InBoundHandlerAdapter {
    private volatile ChannelHandlerContext channel;   // captured when the connection opens

    @Override
    public void channelActive(MessageProcessingContext context) {
        this.channel = context.channel();
    }

    @Override
    public void channelRead(Object message, MessageProcessingContext context) {
        System.out.println("reply: " + message);      // already decoded by the client chain
    }

    /** Thread-safe: the write is handed over to the connection's worker. */
    public void send(String line) {
        channel.writeAndFlush(line + "\n");
    }
}
```

`context.channel().writeAndFlush(...)` walks the outbound handlers (e.g. `StringEncoder`) and
finally `DefaultOutBoundHandler`; `String`, `byte[]` and `ByteBuffer` are all supported, and the
call is safe from any thread.

### 3. Connect with a `Client`

The framework ships a `Client` class for connecting to a remote host and running a handler
pipeline on its own connection. Give it the **same `ManagerGroup` as the server**, so both share
one pool of selector and worker threads:

```java
ClientHandler clientHandler = new ClientHandler();
Client client = new Client("localhost", 8080)
        .setManagerGroup(group)                            // reuse the shared I/O threads
        .handlers(new DelimiterBasedDecoder((byte) '\n'),   // decode the server frames
                  new StringDecoder(),
                  clientHandler);
client.startAsync();                                       // connect; returns immediately

clientHandler.send("hello");                               // write from any thread
// ... the reply arrives in ClientHandler.channelRead ...

client.close();                                            // close this connection
server.close();
group.close();                                             // release all shared I/O threads
```

Both `setManagerGroup` and `handlers` are required (`handlers` takes at least one handler). The
`Future` returned by `startAsync()` completes when the connection ends - peer close, idle timeout
or `close()` - while `start()` blocks the calling thread until then. The client chain is fully
application-supplied: attach whatever decoders the remote side speaks, and since a `Client`
backs exactly one connection, its handler instances are never shared with another connection.

### 4. Test with a raw client socket

A quick sanity check with a plain `java.net.Socket`, i.e. without the `Client` class:

```java
try (Socket socket = new Socket()) {
    socket.connect(new InetSocketAddress("localhost", 8080));
    OutputStream out = socket.getOutputStream();
    out.write("hello\n".getBytes(StandardCharsets.UTF_8));
    out.flush();
}
```

### 5. Run the tests

```bash
mvn test                          # full suite
mvn -Dtest=com.github.skywalker.mininetty.client.FrameworkClientTest test  # Client <-> Server end to end
mvn -Dtest=com.github.skywalker.mininetty.client.ClientTest test           # raw-socket integration tests
```

Performance tests (see `com.github.skywalker.mininetty.client.PerformanceTest`) support a
configurable number of concurrent clients, payload size and timing window:

```bash
mvn -Dtest=com.github.skywalker.mininetty.client.PerformanceTest -Dperf.clients=3 -Dperf.duration=10000 test
```

## Performance

Reference result of `com.github.skywalker.mininetty.client.PerformanceTest` on the development machine: each client is a raw
blocking `Socket` that synchronously sends a 32-byte message and reads the echoed reply, so
throughput is measured as round-trip requests per second (RTT-TPS).

Benchmark environment: **Windows 11 Pro**, AMD Ryzen 5 7500F (6 cores / 12 threads), 32 GB RAM,
Microsoft OpenJDK 17.0.17 (code compiled for Java 8).

| Scenario | Throughput | Avg | p50 | p90 | p99 | Max |
| --- | --- | --- | --- | --- | --- | --- |
| 1 client | 34.0k req/s | 29.2 µs | 26.3 µs | 38.0 µs | 55.7 µs | 1.8 ms |
| 3 concurrent clients | 125.7k req/s total (~ 41-43k each) | 23.8 µs | 22.3 µs | 28.2 µs | 43.0 µs | 1.8 ms |

Settings: 32-byte payload, 2,000 warm-up round trips (JIT), 5 s measurement window.

* Aggregate throughput scales almost linearly with the number of connections (~ 3x at 3 clients):
  every connection is permanently bound to one worker and owns a private handler chain and write
  queue, so concurrent connections introduce no shared-state contention.
* These are single-run observations from a local machine and vary with hardware, JDK, host load
  and payload size - treat them as a rough baseline, not a guarantee.
* The synchronous RTT loop keeps only one request in flight per connection, so per-connection
  throughput is bounded by round-trip latency. To measure higher pure throughput you would
  pipeline several in-flight requests per connection.

## Important design notes

1. **Register user handlers through `HandlerInitializer` and never share handler instances
   between clients.**

   The decoders (`LengthFieldBasedDecoder`, `DelimiterBasedDecoder`, `LineBasedDecoder`) keep
   leftover-buffer state across reads. If a single decoder instance is registered on the server,
   every connection shares that state and frames from one client can corrupt another connection.
   This is exactly why `HandlerInitializer` exists: it creates fresh handler instances for each
   connection.

   Wrong - the same decoder instance is reused by every connection:

   ```java
   DelimiterBasedDecoder shared = new DelimiterBasedDecoder((byte) '\n');
   server.setHandlers(shared, new StringDecoder(), new ResponseHandler());
   ```

   Correct - every connection builds its own chain from `init()`:

   ```java
   server.setHandlers(new HandlerInitializer() {
       @Override
       public Handler[] init() {
           return new Handler[] {
               new DelimiterBasedDecoder((byte) '\n'),
               new StringDecoder(),
               new ResponseHandler()
           };
       }
   });
   ```

   Only truly stateless handlers (no mutable fields, e.g. `StringDecoder`, `StringEncoder`) could
   be shared safely, but using `HandlerInitializer` uniformly is the safest, recommended style.

2. **Built-in handlers are added automatically.** Do not add `LoggingOutboundHandler`,
   `ChannelCleanupInboundHandler`, `IdleDetectionDuplexHandler` or `DefaultOutBoundHandler` to
   your chain - every connection already receives private copies.

3. **Codec constraints.**
   - Decoders process heap (array-backed) `ByteBuffer`s only; direct buffers are ignored.
   - `DelimiterBasedDecoder` supports a single ASCII delimiter.
   - `LengthFieldBasedDecoder` interprets the length field in little-endian by default and throws
     `IllegalStateException` when the announced length exceeds the configured limit.

4. **Writes.** Only `String`, `byte[]` and `ByteBuffer` can reach the socket; other message types
   make `DefaultOutBoundHandler` throw `IllegalStateException`.

5. **Idle timeout.** The default is 5 seconds per connection. Send heartbeats more frequently
   than the timeout if you need longer-lived idle connections.

## Limitations

### Known limitations

- Idle detection treats read-idle and write-idle uniformly instead of separately.
- Write back-pressure is a simple pending-queue threshold, not a high / low watermark pair.
- No SSL/TLS, HTTP, WebSocket or other protocol layers yet - only the codecs listed above.