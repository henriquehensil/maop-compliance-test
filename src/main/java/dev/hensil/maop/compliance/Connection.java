package dev.hensil.maop.compliance;

import com.jlogm.Logger;

import com.jlogm.utils.Coloured;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.model.operation.Message;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.model.operation.Request;

import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.QuicStreamImpl;

import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class Connection implements Closeable {

    // Static initializers

    private static final @NotNull Logger log = Logger.create("Connection");

    static final int GLOBAL_STREAM_LIMIT = 2;
    static final int STREAM_CREATED_BY_PEER_LIMIT = 3;

    // Objects

    private final @NotNull Compliance compliance;
    private final @NotNull QuicClientConnection connection;
    private final @NotNull GlobalOperationsManager manager = new GlobalOperationsManager();
    private final @NotNull Map<Class<? extends DirectionalStream>, Set<DirectionalStream>> streams = new ConcurrentHashMap<>(3, 1f) {{
        this.put(BidirectionalStream.class, ConcurrentHashMap.newKeySet(16));
        this.put(UnidirectionalOutputStream.class, ConcurrentHashMap.newKeySet(16));
        this.put(UnidirectionalInputStream.class, ConcurrentHashMap.newKeySet(16));
    }};

    private final @NotNull AtomicInteger streamCreateCount = new AtomicInteger(0);
    private final @NotNull CompletableFuture<Void> polices = new CompletableFuture<>();

    private volatile boolean authenticated = false;
    private volatile boolean closing = false;

    // Constructor

    Connection(@NotNull QuicClientConnection connection, @NotNull Compliance compliance) {
        this.connection = connection;
        this.compliance = compliance;
        connection.setStreamReadListener(getReadListener());

        this.polices.whenComplete((v, error) -> {
            if (error == null) {
                log.severe("The limit polices was exceeded from connection \"" + this + "\" Preparing to stop diagnostics..");
            } else {
                log.severe("The limit polices was exceeded ( " + error.getMessage() + ") from connection \"" + this + "\" Preparing to stop diagnostics..");
            }

            try {
                this.close();
            } catch (IOException ignore) {

            }

            this.compliance.stop();
        });
    }

    // Getters

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        if (closing) {
            return;
        }

        if (this.authenticated) {
            throw new IllegalStateException("Connection already authenticated. Cannot reverse authenticated connection field");
        }

        this.authenticated = authenticated;
    }

    @NotNull GlobalOperationsManager getManager() {
        return manager;
    }

    public boolean isConnected() {
        return this.connection.isConnected();
    }

    @SuppressWarnings("unchecked")
    public <T extends DirectionalStream> @Nullable T getDirectionalStream(@NotNull Class<? extends T> type, long id) {
        return (T) streams.get(type).stream()
                .filter(s -> s.getId() == id)
                .findFirst()
                .orElse(null);
    }

    // Modules

    public void authenticate() throws IOException, TimeoutException {
        if (authenticated) {
            return;
        }

        @NotNull Authentication authentication = new Authentication(compliance.getPreset());
        @NotNull BidirectionalStream stream = createBidirectionalStream();

        @Nullable ByteBuffer bb = authentication.toByteBuffer();
        stream.write(bb.array(), bb.position(), bb.limit());
        stream.closeOutput();

        bb = null;

        try {
            @NotNull Duration timeout = Duration.ofSeconds(2);
            @NotNull Result result = Result.readResult(stream, timeout);
            if (result instanceof Disapproved disapproved) {
                throw new IOException("Authentication disapproved: " + disapproved);
            }

            // Todo retry_after
        } finally {
            stream.close();
        }
    }

    public @NotNull UnidirectionalOutputStream createUnidirectionalStream() throws DirectionalStreamException {
        @NotNull CompletableFuture<UnidirectionalOutputStream> future = new CompletableFuture<>();
        future.orTimeout(2, TimeUnit.SECONDS);

        @NotNull CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
            log.debug("Creating unidirectional output stream from connection (" + this + ")");

            try {
                @NotNull UnidirectionalOutputStream stream = new UnidirectionalOutputStream(this, this.connection.createStream(false));
                log.info(Coloured.of("NEW Unidirectional stream created by connection (" + this + ") with id: " + stream.getId()).color(Color.orange).print());

                this.streams.get(UnidirectionalOutputStream.class).add(stream);

                future.complete(stream);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                task.cancel(true);
                throw new DirectionalStreamException("Unidirectional Stream creation timeout");
            }

            if (e.getCause() instanceof IOException) {
                throw new DirectionalStreamException("Cannot create unidirectional stream", e);
            }

            throw new AssertionError("Internal error");
        }
    }

    public @NotNull BidirectionalStream createBidirectionalStream() throws DirectionalStreamException {
        @NotNull CompletableFuture<BidirectionalStream> future = new CompletableFuture<>();
        future.orTimeout(2, TimeUnit.SECONDS);

        @NotNull CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
            log.debug("Creating Bidirectional output stream from connection (" + this + ")");

            try {
                @NotNull BidirectionalStream stream = new BidirectionalStream(this, this.connection.createStream(true));
                this.streams.get(BidirectionalStream.class).add(stream);

                log.info(Coloured.of("NEW Bidirectional stream by connection (" + this + ") with id: " + stream.getId()).color(Color.orange).print());

                future.complete(stream);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                task.cancel(true);
                throw new DirectionalStreamException("Bidirectional Stream creation timeout");
            }

            if (e.getCause() instanceof IOException) {
                throw new DirectionalStreamException("Cannot create bidirectional stream", e);
            }

            throw new AssertionError("Internal error");
        }
    }

    public void manage(@NotNull DirectionalStream stream, @NotNull Message message) {
        this.manager.manage(stream, message);
    }

    public void manage(@NotNull DirectionalStream stream, @NotNull Request request) {
        this.manager.manage(stream, request);
    }

    @Blocking
    public @NotNull Operation await(@NotNull Message message, int timeout) throws GlobalOperationManagerException, InterruptedException, TimeoutException {
        @Nullable GlobalOperationsManager.Stage stage = this.getManager().getStage(message);
        if (stage == null) {
            throw new GlobalOperationManagerException("No managed: " + message);
        }

        return stage.await(timeout);
    }

    public @NotNull Operation await(@NotNull Request request, int timeout) throws GlobalOperationManagerException, InterruptedException, TimeoutException {
        @Nullable GlobalOperationsManager.Stage stage = this.getManager().getStage(request);
        if (stage == null) {
            throw new GlobalOperationManagerException("No managed: " + request);
        }

        return stage.await(timeout);
    }

    private @NotNull StreamReadListener getReadListener() {
        return (qs, length) -> {
            if (closing) {
                return;
            }

            if (!(qs instanceof QuicStreamImpl quicStream)) {
                return;
            }

            if (quicStream.isSelfInitiated()) return;

            log.info("New incoming stream: " + quicStream);

            if (quicStream.isUnidirectional()) { // Reject
                log.warn("Illegal unidirectional stream created by peer: " + quicStream);

                if (!closing) {
                    report();

                    try {
                        quicStream.getInputStream().close();
                    } catch (IOException e) {
                        log.trace("Failed to close illegal unidirectional stream: " + e);
                    }
                }

                return;
            }

            // Is a Bidirectional
            @Nullable BidirectionalStream bidirectionalStream = getDirectionalStream(BidirectionalStream.class, quicStream.getStreamId());
            @Nullable GlobalStream globalStream = null;
            boolean isNewbie = !(bidirectionalStream instanceof GlobalStream);

            try {
                if (!isNewbie) {
                    globalStream = (GlobalStream) bidirectionalStream;

                    if (globalStream.hasPendingOperation()) {
                        try {
                            byte @NotNull [] bytes = new byte[Math.toIntExact(length)];
                            int read = globalStream.read(bytes);
                            if (read < bytes.length) {
                                bytes = Arrays.copyOf(bytes, read);
                            }

                            globalStream.fill(bytes);
                            if (globalStream.remaining() == 0) { // Finished
                                @Nullable OperationUtils utils = globalStream.getPendingOperation();
                                if (utils == null) {
                                    throw new AssertionError("Internal error");
                                }

                                @NotNull Operation operation = utils.readOperation(new DataInputStream(new ByteArrayInputStream(bytes)));
                                log.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                                utils.globalHandle(operation, this);
                                globalStream.setAsNonPendingOperation();
                            }

                            return;
                        } catch (ArithmeticException e) {
                            log.severe("An internal error occurred: Global stream is out of control.");

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {}

                            shutdown();
                        } catch (RuntimeException e) {
                            throw new AssertionError("Internal error", e);
                        }
                    }
                }

                if (isNewbie) {
                    if (bidirectionalStream == null) {
                        bidirectionalStream = new BidirectionalStream(this, quicStream);
                        boolean added = streams.get(BidirectionalStream.class).add(bidirectionalStream);
                        if (!added) {
                            throw new AssertionError("Internal error");
                        }

                        log.debug("Number of bidirectional and potential global streams by the connection ( " + this + " ) : " + this.streams.get(BidirectionalStream.class).size());
                    }

                    log.trace("Listen a newbie Bidirectional stream with " + length + " bytes to be read");

                    if (length < Byte.BYTES) {
                        report();
                        return;
                    }

                    byte code = bidirectionalStream.readByte();
                    @Nullable OperationUtils utils = OperationUtils.getByCode(code);
                    boolean illegal = utils == null || !utils.isGlobalManageable();
                    if (illegal) {
                        report();

                        try {
                            bidirectionalStream.close();
                        } catch (IOException e) {
                            log.trace("I/O error when to try close an illegal bidirectional stream: " + bidirectionalStream);
                        }

                        if (utils == null) {
                            log.warn("Illegal operation read by Bidirectional Stream ( " + quicStream + ") with code: " + code);
                            return;
                        }

                        log.warn("Would expect to read a global operation on a bidirectional stream (" + quicStream + ") but was read a " + utils + " operation");
                        return;
                    }

                    // It is a Global stream now
                    globalStream = new GlobalStream(this, quicStream);;
                    this.streams.get(BidirectionalStream.class).remove(bidirectionalStream);
                    this.streams.get(BidirectionalStream.class).add(globalStream);

                    long remaining = length - Byte.BYTES;
                    if (remaining <= 0) {
                        return;
                    }

                    try {
                        byte @NotNull [] bytes = new byte[Math.toIntExact(remaining)];
                        int read = globalStream.read(bytes);
                        if (read < bytes.length) {
                            bytes = Arrays.copyOf(bytes, read);
                        }

                        globalStream.pending(utils);
                        globalStream.fill(bytes);
                        if (globalStream.remaining() == 0) { // Finished
                            @NotNull Operation operation = utils.readOperation(new DataInputStream(new ByteArrayInputStream(bytes)));
                            log.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                            utils.globalHandle(operation, this);
                            globalStream.setAsNonPendingOperation();
                        }
                    } catch (ArithmeticException e) {
                        log.severe("An internal error occurred: Global stream is out of control.");

                        try {
                            globalStream.close();
                        } catch (IOException ignore) {}

                        shutdown();
                    } catch (RuntimeException e) {
                        throw new AssertionError("Internal error", e);
                    }
                }
            } catch (IOException e) {
                if (!closing) {
                    log.warn("I/O error in stream: " + e);

                    if (globalStream != null) {
                        streams.get(BidirectionalStream.class).remove(globalStream);

                        try {
                            globalStream.close();
                        } catch (IOException ignore) {}
                    }

                    streams.get(BidirectionalStream.class).remove(bidirectionalStream);
                    try {
                        bidirectionalStream.close();
                    } catch (IOException ignore) {}
                }
            } catch (Throwable e) {
                log.severe("Unexpected fatal error: " + e.getMessage());
                log.debug().cause(e).log();

                if (!closing) {
                    if (globalStream != null) {
                        streams.get(BidirectionalStream.class).remove(globalStream);

                        try {
                            globalStream.close();
                        } catch (IOException ignore) {}
                    }

                    if (bidirectionalStream != null) {
                        streams.get(BidirectionalStream.class).remove(bidirectionalStream);
                        try {
                            bidirectionalStream.close();
                        } catch (IOException ignore) {}
                    }

                    shutdown();
                }
            }
        };
    }

    /**
     * Help method to increase the stream counter and throws when polices limits are be exceeded.
     * */
    private void report() {
        this.streamCreateCount.incrementAndGet();

        log.debug("Stream count by connection ( " + this + " ) : " + streamCreateCount.get());

        if (isLimitExceeded()) {
            this.polices.complete(null);
        }
    }

    private boolean isLimitExceeded() {
        return streamCreateCount.get() >= STREAM_CREATED_BY_PEER_LIMIT
                || streams.get(BidirectionalStream.class).size() >= GLOBAL_STREAM_LIMIT;
    }

    private void shutdown() {
        if (closing) {
            return;
        }

        log.warn("Shutting down connection: " + this);

        try {
            close();
        } catch (IOException e) {
            log.warn("Error while closing connection: " + e);
        }

        this.compliance.stop();
    }

    @Override
    public void close() throws IOException {
        if (closing) return;

        this.closing = true;
        this.authenticated = false;

        this.compliance.remove(this);

        for (@NotNull Set<DirectionalStream> set : streams.values()) {
            for (@NotNull DirectionalStream stream : set) {
                try {
                    stream.close();
                } catch (IOException ignore) {}
            }
            set.clear();
        }

        this.connection.close();
    }

    @Override
    public @NotNull String toString() {
        return this.connection.toString();
    }
}