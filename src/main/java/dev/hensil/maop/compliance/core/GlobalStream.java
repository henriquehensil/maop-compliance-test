package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;
import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.jetbrains.annotations.UnknownNullability;
import tech.kwik.core.QuicStream;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.NullStreamInputStream;
import tech.kwik.core.stream.QuicStreamImpl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import java.io.InputStream;
import java.nio.ByteBuffer;
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
                if (length == 0) {
                    return;
                }

                {
                    @UnknownNullability InputStream inputStream = quicStream.getInputStream();
                    if (!(inputStream instanceof NullStreamInputStream)) try {
                        if (inputStream.available() == 0) {
                            return;
                        }
                    } catch (IOException ignore) {

                    }
                }

                if (quicStream.isSelfInitiated()) {
                    @Nullable DirectionalStreamObserver observer = connection.getObserver(quicStream.getStreamId());
                    if (observer == null) {
                        throw new AssertionError("Internal error");
                    }

                    if (observer.isWaitReading()) {
                        log.trace("Firing new readings in the Observer (stream = " + quicStream.getStreamId() + " & length = " + length + ")");
                        observer.fireReading(length);
                    }

                    return;
                }

                if (quicStream.isUnidirectional()) { // Reject
                    log.warn("Useless unidirectional stream created by peer: " + quicStream);

                    if (!connection.isClosed()) {
                        log.trace("Reporting " + quicStream);
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
                        log.trace("Reporting " + quicStream);
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

                            log.trace("Reporting " + quicStream);
                            connection.reportGlobalPolicies();
                        }

                        log.info("New incoming bidirectional stream with id " + quicStream.getStreamId());
                        bidirectionalStream = new BidirectionalStream(connection, quicStream);

                        boolean added = streams.add(bidirectionalStream);
                        if (!added) {
                            throw new AssertionError("Internal error");
                        }

                        log.debug("Number of potential global streams by the connection " + connection + ": " + streams.size());
                        log.trace("Reading new operation from potential global stream with id " + bidirectionalStream.getId());

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
                                    log.trace("Illegal operation read by Bidirectional Stream ( " + quicStream + ") with code: " + code);
                                } else {
                                    log.trace("Would expect to read a Global operation on a bidirectional stream (" + quicStream + ") but a " + utils + " operation was read");
                                }

                                log.trace("Reporting " + quicStream);
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
                            log.trace("New Global stream (" + quicStream + ") was defined with " + length + " bytes available");

                            try {
                                log.trace("New " + utils + " operation is pending to be completed");
                                globalStream.pending(utils);

                                if (globalStream.isComplete()) { // Finished
                                    @Nullable ByteBuffer buffer = globalStream.getBuffer();
                                    if (buffer == null) {
                                        throw new AssertionError("Internal error");
                                    }

                                    @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit())));
                                    log.trace("Successfully read operation on newbie global stream (" + quicStream + ") : " + operation);
                                    utils.handleObserve(operation, connection);
                                    globalStream.resetAll();
                                }
                            } catch (Throwable e) {
                                log.severe("Fatal error occurs while read global operation operation: " + e.getMessage());
                                log.debug().cause(e).log();
                                shutdown(connection);
                            }
                        }
                    }

                    // It is not newbie.

                    else {
                        globalStream = (GlobalStream) bidirectionalStream;
                        log.trace("Non newbie Global stream (" + quicStream + ") with " + globalStream.available() + " bytes available received");

                        if (globalStream.hasPendingOperation()) {
                            @Nullable OperationUtil util = globalStream.pendingOperation;
                            @Nullable ByteBuffer buffer = globalStream.getBuffer();
                            if (util == null || buffer == null) {
                                throw new AssertionError("Internal error");
                            }

                            log.trace("Trying to finish read " + util + " operation");
                            try (
                                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                            .put("operation code", util.getCode())
                                            .put("buffer capacity", buffer.capacity())
                                            .install();

                                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Read operation")
                            ) {
                                globalStream.proceed();

                                if (globalStream.isComplete()) {
                                    log.trace(util + " operation ready to be complete on " + globalStream);

                                    @NotNull Operation operation = util.read(new DataInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit())));
                                    log.trace("Successfully read operation on the global stream (" + quicStream + ") : " + operation);
                                    util.handleObserve(operation, connection);
                                    globalStream.resetAll();
                                }
                            } catch (Throwable e) {
                                log.severe("Fatal error occurs while read global operation operation: " + e.getMessage());
                                log.debug().cause(e).log();
                            }
                        } else {
                            log.trace("Reading new operation from Global stream with id " + globalStream.getId());

                            byte code = globalStream.readByte();
                            @Nullable OperationUtil utils = OperationUtil.getByCode(code);
                            boolean illegal = utils == null || !utils.isGlobalOperation();

                            if (utils == OperationUtil.DISCONNECT || utils == OperationUtil.DISCONNECT_REQUEST) {
                                log.severe("Received a disconnect operation without reason: " + utils);
                                shutdown(connection);
                            }

                            if (illegal) {
                                if (utils == null) {
                                    log.warn("Illegal operation read by Global Stream ( " + quicStream + ") with code: " + code);
                                } else {
                                    log.warn("Would expect to read a global operation on a bidirectional stream (" + quicStream + ") but a " + utils + " operation was read");
                                }

                                log.trace("Reporting " + quicStream);
                                connection.reportGlobalPolicies();

                                try {
                                    globalStream.close();
                                } catch (IOException e) {
                                    log.trace("I/O error occurred while trying to close an illegal bidirectional stream with id" + bidirectionalStream.getId() + "): " + e);
                                }

                                return;
                            }

                            try {
                                log.trace("New " + utils + " operation is pending to be completed");
                                globalStream.pending(utils);

                                if (globalStream.isComplete()) {
                                    log.trace(utils + " operation ready to be complete on " + globalStream);

                                    @Nullable ByteBuffer buffer = globalStream.getBuffer();
                                    if (buffer == null) {
                                        throw new AssertionError("Internal error");
                                    }

                                    @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit())));
                                    log.trace("Successfully read operation on the global stream (" + quicStream + ") : " + operation);
                                    utils.handleObserve(operation, connection);
                                    globalStream.resetAll();
                                }
                            } catch (Throwable e) {
                                log.severe("Fatal error occurs while read global operation operation: " + e.getMessage());
                                log.debug().cause(e).log();
                            }
                        }
                    }
                } catch (IOException e) {
                    if (!connection.isClosed()) {
                        log.trace("I/O error in stream: " + e);

                        if (globalStream != null) {
                            streams.remove(globalStream);

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {

                            }

                            return;
                        }

                        streams.remove(bidirectionalStream);
                        try {
                            bidirectionalStream.close();
                        } catch (IOException ignore) {}
                    }
                } catch (UnsupportedOperationException e) {
                    log.severe("Global stream read invalid operation: " + e.getMessage());
                    shutdown(connection);
                } catch (Throwable e) {
                    log.severe("Unexpected error: " + e.getMessage());
                    log.debug().cause(e).log();
                    shutdown(connection);
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

    private @Nullable OperationUtil pendingOperation;
    private @Nullable ByteBuffer buffer;
    private boolean completed = false;

    GlobalStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        super(connection, stream);
    }

    // Getters

    @Nullable ByteBuffer getBuffer() {
        return buffer;
    }

    boolean hasPendingOperation() {
        return pendingOperation != null;
    }

    public boolean isComplete() {
        return hasPendingOperation() && completed;
    }

    // Modules

    void pending(@NotNull OperationUtil util) throws IOException {
        this.pendingOperation = util;
        this.buffer = util.readGlobal(this);
    }

    void proceed() throws IOException {
        if (pendingOperation != null) {
            this.buffer = pendingOperation.readGlobal(this);
        }
    }

    void complete() {
        this.completed = true;
    }

    void resetAll() {
        this.pendingOperation = null;
        this.buffer = null;
    }
}