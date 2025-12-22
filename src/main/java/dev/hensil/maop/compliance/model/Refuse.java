package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

public final class Refuse extends Operation {

    private final @NotNull Entry @NotNull [] entries;

    public Refuse(@NotNull Entry @NotNull [] entries) {
        super((byte) 0x04);
        this.entries = entries;
    }

    public @NotNull Entry @NotNull [] getEntries() {
        return entries;
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