package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Refuse extends Operation {

    private final @NotNull Entry @NotNull [] entries;

    public Refuse(@NotNull Entry @NotNull [] entries) {
        super((byte) 0x04);
        this.entries = entries;
    }

    public @NotNull Entry @NotNull [] getEntries() {
        return entries;
    }

    @Override
    public byte @NotNull [] toBytes() {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(entries.length * (8 + 4 + 2));

        for (@NotNull Entry entry : entries) {
            buffer.putLong(entry.stream)
                    .putInt(entry.retryAfter)
                    .putShort(entry.errorCode);
        }

        return buffer.array();
    }

    // Classes

    public static final class Entry {

        private final long stream;
        private final int retryAfter;
        private final short errorCode;

        public Entry(long stream, int retryAfter, short errorCode) {
            this.stream = stream;
            this.retryAfter = retryAfter;
            this.errorCode = errorCode;
        }

        public long getStream() {
            return stream;
        }

        public int getRetryAfter() {
            return retryAfter;
        }

        public short getErrorCode() {
            return errorCode;
        }
    }
}