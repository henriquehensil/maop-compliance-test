package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;
import tech.kwik.core.QuicStream;

import java.nio.ByteBuffer;

public final class GlobalStream extends BidirectionalStream {

    private @Nullable ByteBuffer buffer;
    private @Nullable OperationUtils utils = null;

    GlobalStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        super(connection, stream);
    }

    // Getters

    boolean hasPendingOperation() {
        return utils != null;
    }

    public int remaining() {
        if (!hasPendingOperation()) {
            throw new IllegalStateException("No pending operation");
        }

        if (buffer == null) {
            return utils.getHeaderLength();
        }

        return utils.getHeaderLength() - buffer.position();
    }

    @Nullable OperationUtils getPendingOperation() {
        return utils;
    }

    // Modules

    void pending(@NotNull OperationUtils utils) {
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