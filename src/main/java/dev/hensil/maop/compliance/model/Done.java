package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Done extends Operation {

    private final short count;
    private final @NotNull Entry @NotNull [] entries;

    public Done(@NotNull Entry @NotNull [] entries) {
        this((short) entries.length, entries);
    }

    public Done(short count, @NotNull Entry @NotNull [] entries) {
        super((byte) 0x03);
        this.entries = entries;
        this.count = count;
    }

    public @NotNull Entry @NotNull [] getEntries() {
        return entries;
    }

    @Override
    public byte @NotNull [] toBytes() {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(2 + (entries.length * (8 + 8 + 4)))
                .putShort(count);

        for (@NotNull Entry entry : entries) {
            buffer.putLong(entry.stream)
                    .putLong(entry.start)
                    .putInt(entry.end);
        }

        return buffer.array();
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
