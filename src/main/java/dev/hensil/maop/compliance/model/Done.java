package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

public final class Done extends Operation {

    private final @NotNull Entry @NotNull [] entries;

    public Done(@NotNull Entry @NotNull [] entries) {
        super((byte) 0x03);
        this.entries = entries;
    }

    public @NotNull Entry @NotNull [] getEntries() {
        return entries;
    }

    // Classes

    public static final class Entry {

        private final long stream;
        private final long start;
        private final int end;

        public Entry(long stream, long start, int end) {
            this.stream = stream;
            this.start = start;
            this.end = end;
        }

        public long getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        public long getStream() {
            return stream;
        }
    }
}
