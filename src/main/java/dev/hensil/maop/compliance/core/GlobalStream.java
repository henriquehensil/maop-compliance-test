package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;
import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicStream;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.QuicStreamImpl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

public final class GlobalStream extends BidirectionalStream {

    // Static initializers

    private static final @NotNull Logger log = Logger.create(GlobalStream.class);

    static @NotNull StreamReadListener newGlobalListener(@NotNull Connection connection) {
        return (qs, length) -> {
            if (connection.isClosed()) {
                return;
            }

            if (!(qs instanceof QuicStreamImpl quicStream)) {
                return;
            }

            try (
                    @NotNull LogCtx.Scope logContext = LogCtx.builder()
                            .put("compliance id", connection.getCompliance().getId())
                            .put("connection closed", connection.isClosed())
                            .put("stream id", quicStream.getStreamId())
                            .put("bidirectional", quicStream.isBidirectional())
                            .put("bytes to read", length)
                            .put("self created", quicStream.isSelfInitiated())
                            .install();

                    @NotNull Stack.Scope logScope = Stack.pushScope("Global stream listener")
            ) {
                if (quicStream.isSelfInitiated()) {
                    @Nullable DirectionalStreamObserver observer = connection.getObserver(quicStream.getStreamId());
                    if (observer == null) {
                        throw new AssertionError("Internal error");
                    }

                    if (observer.isWaitReading()) {
                        log.trace("Firing new readings in the Observer (stream = " + quicStream + ")");
                        observer.fireReading();
                    }

                    return;
                }

                if (quicStream.isUnidirectional()) { // Reject
                    log.warn("Useless unidirectional stream created by peer: " + quicStream);

                    if (!connection.isClosed()) {
                        log.trace("Reporting global policies (stream = " + quicStream + ")");
                        connection.reportGlobalPolicies();

                        try {
                            quicStream.getInputStream().close();
                        } catch (IOException e) {
                            log.trace("Failed to close useless unidirectional stream with id " + quicStream.getStreamId() + ": " + e);
                        }
                    }

                    return;
                }

                if (!connection.isAuthenticated()) {
                    log.warn("Illegal stream created by peer (stream id = " + quicStream.getStreamId() + "): Not authenticated connection");

                    if (!connection.isClosed()) {
                        log.trace("Reporting global policies (stream = " + quicStream + ")");
                        connection.reportGlobalPolicies();

                        try {
                            quicStream.getInputStream().close();
                            quicStream.getOutputStream().close();
                        } catch (IOException e) {
                            log.trace("Failed to close illegal stream with id " + quicStream.getStreamId() + ": " + e);
                        }
                    }

                    return;
                }

                if (length == 0) {
                    return;
                }

                @Nullable BidirectionalStream bidirectionalStream = connection.getDirectionalStream(BidirectionalStream.class, quicStream.getStreamId());
                @Nullable GlobalStream globalStream = null;
                boolean isNewbie = bidirectionalStream == null;

                @NotNull Set<DirectionalStream> streams = connection.getStreams().get(BidirectionalStream.class);
                try {
                    if (isNewbie) {
                        if (length > Short.MAX_VALUE) {
                            log.warn("Too many bytes in a newbie global stream");

                            try {
                                quicStream.getInputStream().close();
                                quicStream.getOutputStream().close();
                            } catch (IOException e) {
                                log.trace("I/O error occurred while trying to close an illegal bidirectional stream with id" + quicStream.getStreamId() + "): " + e);
                            }

                            log.trace("Reporting global policies (stream = " + quicStream + ")");
                            connection.reportGlobalPolicies();
                        }

                        log.info("New incoming bidirectional stream with id " + quicStream.getStreamId());
                        bidirectionalStream = new BidirectionalStream(connection, quicStream);

                        boolean added = streams.add(bidirectionalStream);
                        if (!added) {
                            throw new AssertionError("Internal error");
                        }

                        log.debug("Number of potential global streams by the connection " + connection + ": " + streams.size());

                        byte code = bidirectionalStream.readByte();
                        @Nullable OperationUtil utils = OperationUtil.getByCode(code);
                        boolean illegal = utils == null || !utils.isGlobalOperation();

                        if (utils == OperationUtil.DISCONNECT || utils == OperationUtil.DISCONNECT_REQUEST) {
                            log.severe("Received a disconnect operation without reason: " + utils);
                            shutdown(connection);
                        }

                        try (
                                @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                        .put("operation code", code)
                                        .put("illegal operation", illegal)
                                        .install();

                                @NotNull Stack.Scope logScope2 = Stack.pushScope("Read newbie operation")
                        ) {
                            if (illegal) {
                                if (utils == null) {
                                    log.warn("Illegal operation read by Bidirectional Stream ( " + quicStream + ") with code: " + code);
                                } else {
                                    log.warn("Would expect to read a global operation on a bidirectional stream (" + quicStream + ") but a " + utils + " operation was read");
                                }

                                log.trace("Reporting global policies (stream = " + quicStream + ")");
                                connection.reportGlobalPolicies();

                                try {
                                    bidirectionalStream.close();
                                } catch (IOException e) {
                                    log.trace("I/O error occurred while trying to close an illegal bidirectional stream with id" + bidirectionalStream.getId() + "): " + e);
                                }

                                return;
                            }

                            // It is a Global stream now
                            streams.remove(bidirectionalStream);
                            globalStream = new GlobalStream(connection, quicStream);
                            streams.add(globalStream);

                            globalStream.pending(utils);

                            long remaining = length - 1;
                            if (remaining < utils.getHeaderLength()) {
                                return;
                            }

                            try {
                                byte @NotNull [] bytes = new byte[utils.getHeaderLength()];
                                int read = globalStream.read(bytes);
                                if (read < bytes.length) {
                                    bytes = Arrays.copyOf(bytes, read);
                                }

                                globalStream.fill(bytes);

                                if (globalStream.remaining() == 0) { // Finished
                                    @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(bytes)));
                                    log.trace("Successfully read operation on newbie global stream (" + quicStream + ") : " + operation);
                                    utils.handleObserve(operation, connection);
                                    globalStream.setAsNonPendingOperation();
                                }

                            } catch (RuntimeException e) {
                                throw new AssertionError("Internal error", e);
                            }
                        }
                    }

                    // It is not newbie.

                    else {
                        globalStream = (GlobalStream) bidirectionalStream;

                        if (globalStream.hasPendingOperation()) {
                            @Nullable OperationUtil utils = globalStream.getPendingOperation();
                            if (utils == null) {
                                throw new AssertionError("Internal error");
                            }

                            if (length < globalStream.remaining()) {
                                return;
                            }

                            try (
                                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                            .put("operation code", utils.getCode())
                                            .put("operation name", utils.getName())
                                            .put("remaining", globalStream.remaining())
                                            .install();

                                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Read operation")
                            ) {
                                byte @NotNull [] bytes = new byte[globalStream.remaining()];
                                int read = globalStream.read(bytes);
                                if (read < bytes.length) {
                                    bytes = Arrays.copyOf(bytes, read);
                                }

                                globalStream.fill(bytes);
                                if (globalStream.remaining() == 0) {// Finished
                                    @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(bytes)));
                                    log.trace("New operation read on global stream (" + quicStream + ") : " + operation);

                                    utils.handleObserve(operation, connection);
                                    globalStream.setAsNonPendingOperation();
                                }
                            } catch (RuntimeException e) {
                                throw new AssertionError("Internal error", e);
                            }
                        } else {
                            byte code = globalStream.readByte();
                            @Nullable OperationUtil utils = OperationUtil.getByCode(code);
                            boolean illegal = utils == null || !utils.isGlobalOperation();

                            if (utils == OperationUtil.DISCONNECT || utils == OperationUtil.DISCONNECT_REQUEST) {
                                log.severe("Received a disconnect operation without reason: " + utils);
                                shutdown(connection);
                            }

                            if (illegal) {
                                if (utils == null) {
                                    log.warn("Illegal operation read by Bidirectional Stream ( " + quicStream + ") with code: " + code);
                                } else {
                                    log.warn("Would expect to read a global operation on a bidirectional stream (" + quicStream + ") but a " + utils + " operation was read");
                                }

                                log.trace("Reporting global policies (stream = " + quicStream + ")");
                                connection.reportGlobalPolicies();

                                try {
                                    bidirectionalStream.close();
                                } catch (IOException e) {
                                    log.trace("I/O error occurred while trying to close an illegal bidirectional stream with id" + bidirectionalStream.getId() + "): " + e);
                                }

                                return;
                            }

                            long remaining = length - 1;
                            if (remaining < utils.getHeaderLength()) {
                                return;
                            }

                            try {
                                byte @NotNull [] bytes = new byte[utils.getHeaderLength()];
                                int read = globalStream.read(bytes);
                                if (read < bytes.length) {
                                    bytes = Arrays.copyOf(bytes, read);
                                }

                                globalStream.fill(bytes);

                                if (globalStream.remaining() == 0) { // Finished
                                    @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(bytes)));
                                    log.trace("Successfully read operation on newbie global stream (" + quicStream + ") : " + operation);
                                    utils.handleObserve(operation, connection);
                                    globalStream.setAsNonPendingOperation();
                                }

                            } catch (RuntimeException e) {
                                throw new AssertionError("Internal error", e);
                            }
                        }
                    }
                } catch (IOException e) {
                    if (!connection.isClosed()) {
                        log.warn("I/O error in stream: " + e);

                        if (globalStream != null) {
                            streams.remove(globalStream);

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {}
                        }

                        streams.remove(bidirectionalStream);
                        try {
                            bidirectionalStream.close();
                        } catch (IOException ignore) {}
                    }
                } catch (Throwable e) {
                    log.severe("Unexpected fatal error: " + e.getMessage());
                    log.debug().cause(e).log();

                    if (!connection.isClosed()) {
                        if (globalStream != null) {
                            streams.remove(globalStream);

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {}
                        }

                        if (bidirectionalStream != null) {
                            streams.remove(bidirectionalStream);
                            try {
                                bidirectionalStream.close();
                            } catch (IOException ignore) {}
                        }

                        shutdown(connection);
                    }
                }
            }
        };
    }

    private static void shutdown(@NotNull Connection connection) {
        if (connection.isClosed()) {
            return;
        }

        log.warn("Shutting down connection: " + connection);

        try {
            connection.close();
        } catch (IOException e) {
            log.warn("Error while closing connection: " + e);
        }

        connection.getCompliance().stop();
    }

    // Objects

    private @Nullable ByteBuffer buffer;
    private @Nullable OperationUtil utils = null;

    GlobalStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        super(connection, stream);
    }

    // Getters

    boolean hasPendingOperation() {
        return utils != null;
    }

    int remaining() {
        if (!hasPendingOperation()) {
            throw new IllegalStateException("No pending operation");
        }

        if (buffer == null) {
            return utils.getHeaderLength();
        }

        return utils.getHeaderLength() - buffer.position();
    }

    @Nullable OperationUtil getPendingOperation() {
        return utils;
    }

    // Modules

    void pending(@NotNull OperationUtil utils) {
        this.utils = utils;
    }

    void fill(byte @NotNull [] bytes) {
        if (!hasPendingOperation()) {
            throw new IllegalStateException("No pending operation");
        }

        if (buffer == null) {
            buffer = ByteBuffer.allocate(utils.getHeaderLength());
        }

        buffer.put(bytes, 0, buffer.limit());
    }

    void setAsNonPendingOperation() {
        this.buffer = null;
        this.utils = null;
    }
}