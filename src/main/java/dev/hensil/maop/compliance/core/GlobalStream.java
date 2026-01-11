package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;
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

            if (quicStream.isSelfInitiated()) return;

            log.info("New incoming stream: " + quicStream);

            if (quicStream.isUnidirectional()) { // Reject
                log.warn("Illegal unidirectional stream created by peer: " + quicStream);

                if (!connection.isClosed()) {
                    connection.reportGlobalPolices();

                    try {
                        quicStream.getInputStream().close();
                    } catch (IOException e) {
                        log.trace("Failed to close illegal unidirectional stream: " + e);
                    }
                }

                return;
            }

            // Is a Bidirectional
            @Nullable BidirectionalStream bidirectionalStream = connection.getDirectionalStream(BidirectionalStream.class, quicStream.getStreamId());
            @Nullable GlobalStream globalStream = null;
            boolean isNewbie = !(bidirectionalStream instanceof GlobalStream);

            @NotNull Set<DirectionalStream> streams = connection.getStreams().get(BidirectionalStream.class);
            try {
                if (isNewbie) {
                    if (bidirectionalStream == null) {
                        bidirectionalStream = new BidirectionalStream(connection, quicStream);

                        boolean added = streams.add(bidirectionalStream);
                        if (!added) {
                            throw new AssertionError("Internal error");
                        }

                        log.debug("Number of bidirectional and potential global streams by the connection ( " + connection + " ) : " + streams.size());
                    }

                    log.trace("Listen a newbie Bidirectional stream with " + length + " bytes to be read");

                    if (length < Byte.BYTES) {
                        connection.reportGlobalPolices();
                        return;
                    }

                    byte code = bidirectionalStream.readByte();
                    @Nullable OperationUtil utils = OperationUtil.getByCode(code);
                    boolean illegal = utils == null || !utils.isGlobalOperation();
                    if (illegal) {
                        if (utils == OperationUtil.DISCONNECT || utils == OperationUtil.DISCONNECT_REQUEST) {
                            log.severe("Receive a disconnect operation without reason: " + utils);
                            shutdown(connection);
                        }

                        connection.reportGlobalPolices();

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
                    streams.remove(bidirectionalStream);
                    globalStream = new GlobalStream(connection, quicStream);

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
                            @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(bytes)));
                            log.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                            utils.handleObserve(operation, connection);
                            globalStream.setAsNonPendingOperation();
                        }

                        streams.add(globalStream);
                    } catch (ArithmeticException e) {
                        log.severe("An internal error occurred: Global stream is out of control.");

                        try {
                            globalStream.close();
                        } catch (IOException ignore) {}

                        shutdown(connection);
                    } catch (RuntimeException e) {
                        throw new AssertionError("Internal error", e);
                    }
                }

                // It is not newbie.

                else {
                    globalStream = (GlobalStream) bidirectionalStream;

                    if (globalStream.hasPendingOperation()) {
                        try {
                            byte @NotNull [] bytes = new byte[globalStream.remaining()];
                            int read = globalStream.read(bytes);
                            if (read < bytes.length) {
                                bytes = Arrays.copyOf(bytes, read);
                            }

                            globalStream.fill(bytes);
                            if (globalStream.remaining() == 0) { // Finished
                                @Nullable OperationUtil utils = globalStream.getPendingOperation();
                                if (utils == null) {
                                    throw new AssertionError("Internal error");
                                }

                                if (utils == OperationUtil.DISCONNECT || utils == OperationUtil.DISCONNECT_REQUEST) {
                                    log.severe("Receive a disconnect operation without reason: " + utils);
                                    shutdown(connection);
                                }

                                @NotNull Operation operation = utils.read(new DataInputStream(new ByteArrayInputStream(bytes)));
                                log.trace("New operation read on bidirectional stream (" + quicStream + ") : " + operation);

                                utils.handleObserve(operation, connection);
                                globalStream.setAsNonPendingOperation();
                            }
                        } catch (ArithmeticException e) {
                            log.severe("An internal error occurred: Global stream is out of control.");

                            try {
                                globalStream.close();
                            } catch (IOException ignore) {}

                            shutdown(connection);
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