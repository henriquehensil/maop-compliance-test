package dev.hensil.maop.compliance;

import com.jlogm.Logger;
import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicConnection;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.QuicStreamImpl;

import java.io.Closeable;
import java.io.IOException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Connection implements Closeable {

    // Static initializers

    public static @NotNull Logger logStream = Logger.create("Global Stream");
    public static @NotNull Logger log = Logger.create("Connection");

    private static final int GLOBAL_STREAM_LIMIT = 2;
    private static final int STREAM_CREATED_BY_PEER_LIMIT = 4;

    // Objects

    private final @NotNull Compliance compliance;

    private final @NotNull QuicConnection connection;
    private final @NotNull GlobalOperationsManager manager = new GlobalOperationsManager();
    private final @NotNull Map<Class<? extends DirectionalStream>, Set<DirectionalStream>> streams = new ConcurrentHashMap<>(3, 1f) {{
        this.put(BidirectionalStream.class, new HashSet<>(16, 1f));
        this.put(UnidirectionalOutputStream.class, new HashSet<>(16, 1f));
        this.put(UnidirectionalInputStream.class, new HashSet<>(16, 1f));
    }};

    private int streamCreateCount = 0;
    private volatile boolean closing = false;

    // Constructor

    protected Connection(@NotNull QuicConnection connection, @NotNull Compliance compliance) {
        this.connection = connection;
        this.compliance = compliance;
        connection.setStreamReadListener(getReadListener());
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

    protected @NotNull StreamReadListener getReadListener() {
        return (qs, l) -> this.compliance.getExecutor().execute(() -> {
            @NotNull QuicStreamImpl quicStream;
            if (!(qs instanceof QuicStreamImpl)) {
                throw new AssertionError("Internal error");
            }

            quicStream = (QuicStreamImpl) qs;
            if (quicStream.isSelfInitiated()) {
                return;
            }

            try (
                    @NotNull LogCtx.Scope ctx = LogCtx.builder()
                            .put("stream id", quicStream.getStreamId())
                            .put("bidirectional", quicStream.isBidirectional())
                            .install();

                    @NotNull Stack.Scope scope = Stack.pushScope("Validation");
            ) {
                if (quicStream.isUnidirectional()) {
                    log.info("A unidirectional connection was created remotely in vain. Increasing the counter.");

                    try {
                        quicStream.getInputStream().close();
                    } catch (IOException e) {
                        log.warn().cause(e).log("Cannot close this connection: " + this + " Ignoring...");
                    }

                    this.streamCreateCount++;
                }

                if (quicStream.isBidirectional()) {
                    log.warn("A new bidirectional Stream was created remotely");

                    final @NotNull BidirectionalStream stream = new BidirectionalStream(this, quicStream);
                    this.streams.get(BidirectionalStream.class).add(stream);

                    boolean isLimitExceeded = streamCreateCount >= STREAM_CREATED_BY_PEER_LIMIT || this.streams.get(BidirectionalStream.class).size() >= GLOBAL_STREAM_LIMIT;

                    if (isLimitExceeded) {
                        log.severe("The limit for creating any stream content has been exceeded. Closing connection" + this);

                        try {
                            quicStream.getInputStream().close();
                            quicStream.getOutputStream().close();
                        } catch (IOException ignore) {
                            // ignore
                        }

                        try {
                            this.close();
                        } catch (IOException e) {
                            log.warn().cause(e).log("Cannot close this connection: " + this + " Ignoring...");
                        }

                        this.compliance.stop();
                        return;
                    }

                    this.compliance.getExecutor().execute(() -> {
                        log.trace("Trying to identify an Global Stream from the new Bidirectional Stream created");

                        while (!Thread.currentThread().isInterrupted()) try {
                            byte code = stream.readByte();
                            @Nullable OperationUtils utils = OperationUtils.getByCode(code);

                            try (
                                    @NotNull LogCtx.Scope ctx2 = LogCtx.builder()
                                            .put("operation code", code)
                                            .put("valid", utils != null)
                                            .install();

                                    @NotNull Stack.Scope scope2 = Stack.pushScope("Verification");
                            ) {
                                if (utils == null) {
                                    log.warn("An invalid operation was read in a bidirectional stream. Closing the stream and increase the counter.");

                                    try {
                                        stream.close();
                                    } catch (IOException e) {
                                        log.trace().cause(new DirectionalStreamException(e.getMessage())).log("Cannot close an illegal bidirectional stream");
                                    }

                                    this.streamCreateCount++;

                                    return;
                                }

                                if (!utils.isGlobalManageable()) {
                                    log.warn("A bidirectional stream was created without purpose. Closing the stream and increase the counter.");

                                    try {
                                        stream.close();
                                    } catch (IOException e) {
                                        log.trace().cause(new DirectionalStreamException(e.getMessage())).log("Cannot close an unusable bidirectional stream");
                                    }

                                    this.streamCreateCount++;

                                    return;
                                }

                                @NotNull Operation operation = utils.readOperation(stream);
                                stream.setGlobal(true);
                                this.streams.get(BidirectionalStream.class).add(stream);

                                utils.globalHandle(operation, this);

                                logStream.info("New Global Stream created by connection");
                            }
                        } catch (IOException e) {
                            if (!closing) {
                                try {
                                    stream.close();
                                } catch (IOException ignore) {
                                    // ignored
                                }

                                return;
                            }

                            logStream.trace().cause(e).log("A error occurred while to try read a global operation in Bidirectional Stream");
                            this.streams.get(BidirectionalStream.class).remove(stream);

                            this.streamCreateCount++;
                        } catch (GlobalOperationManagerException e) {
                            logStream.severe().cause(e).log(e.getMessage() + ".. Closing connection");

                            try {
                                stream.close();
                                this.close();
                            } catch (IOException ex) {
                                log.trace().cause(e).log(ex);
                            }

                            this.compliance.stop();
                            return;
                        }
                    });
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        this.closing = true;

        for (@NotNull Set<DirectionalStream> streams : this.streams.values()) {
            for (@NotNull DirectionalStream stream : streams) {
                streams.remove(stream);
                stream.close();
            }
        }

        this.connection.close();
    }
}