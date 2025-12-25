package dev.hensil.maop.compliance;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicConnection;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.QuicStreamImpl;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class Connection implements Closeable {

    // Static initializers

    private static final @NotNull Logger logStream = Logger.create("Global Stream");
    private static final @NotNull Logger log = Logger.create("Connection");

    static final int GLOBAL_STREAM_LIMIT = 2;
    static final int STREAM_CREATED_BY_PEER_LIMIT = 3;

    // Objects

    private final @NotNull Compliance compliance;
    private final @NotNull QuicConnection connection;
    private final @NotNull GlobalOperationsManager manager = new GlobalOperationsManager();
    private final @NotNull Map<Class<? extends DirectionalStream>, Set<DirectionalStream>> streams = new ConcurrentHashMap<>(3, 1f) {{
        this.put(BidirectionalStream.class, ConcurrentHashMap.newKeySet(16));
        this.put(UnidirectionalOutputStream.class, ConcurrentHashMap.newKeySet(16));
        this.put(UnidirectionalInputStream.class, ConcurrentHashMap.newKeySet(16));
    }};

    private final @NotNull AtomicInteger streamCreateCount = new AtomicInteger(0);
    private final @NotNull CompletableFuture<Void> polices = new CompletableFuture<>();

    private volatile boolean closing = false;

    // Constructor

    Connection(@NotNull QuicConnection connection, @NotNull Compliance compliance) {
        this.connection = connection;
        this.compliance = compliance;
        connection.setStreamReadListener(getReadListener());

        this.polices.whenComplete((v, error) -> {
            if (error == null) {
                log.severe("The limit polices was exceeded from connection \"" + this + "\" Preparing to stop diagnostics..");
            } else {
                log.severe("The limit polices was exceeded ( " + error.getMessage() + ") from connection \"" + this + "\" Preparing to stop diagnostics..");
            }

            this.compliance.stop();
        });
    }

    // Getters

    @NotNull GlobalOperationsManager getManager() {
        return manager;
    }

    @SuppressWarnings("unchecked")
    public <T extends DirectionalStream> @Nullable T getDirectionalStream(@NotNull Class<? extends T> type, long id) {
        return (T) streams.get(type).stream()
                .filter(s -> s.getId() == id)
                .findFirst()
                .orElse(null);
    }

    // Modules

    public @NotNull UnidirectionalOutputStream createUnidirectionalStream() throws DirectionalStreamException {
        try {
            @NotNull UnidirectionalOutputStream stream = new UnidirectionalOutputStream(this, this.connection.createStream(false));

            this.streams.get(UnidirectionalOutputStream.class).add(stream);

            return stream;
        } catch (IOException e) {
            throw new DirectionalStreamException("A error occurs while to trying create a unidirectional output stream", e);
        }
    }

    public @NotNull BidirectionalStream createBidirectionalStream() throws DirectionalStreamException {
        try {
            @NotNull BidirectionalStream stream = new BidirectionalStream(this, this.connection.createStream(false));

            this.streams.get(BidirectionalStream.class).add(stream);

            return stream;
        } catch (IOException e) {
            throw new DirectionalStreamException("A error occurs while to trying create a bidirectional stream", e);
        }
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
                                logStream.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                                utils.globalHandle(operation, this);
                                globalStream.setAsNonPendingOperation();
                            }

                            return;
                        } catch (ArithmeticException e) {
                            logStream.severe("An internal error occurred: Global stream is out of control.");

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {}

                            shutdown();
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
                            log.warn("Illegal operation read by Bidirectional Stream: " + code);
                            return;
                        }

                        log.warn("Would expect to read a global operation on a bidirectional stream");
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

                        globalStream.fill(bytes);
                        if (globalStream.remaining() == 0) { // Finished
                            @NotNull Operation operation = utils.readOperation(new DataInputStream(new ByteArrayInputStream(bytes)));
                            logStream.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                            utils.globalHandle(operation, this);
                            globalStream.setAsNonPendingOperation();
                        }
                    } catch (ArithmeticException e) {
                        logStream.severe("An internal error occurred: Global stream is out of control.");

                        try {
                            globalStream.close();
                        } catch (IOException ignore) {}

                        shutdown();
                    }
                }
            } catch (IOException e) {
                if (!closing) {
                    logStream.warn("I/O error in stream: " + e);

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
            } catch (Exception e) {
                logStream.severe("Fatal error: " + e);

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

        log.severe("Shutting down connection: " + this);

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