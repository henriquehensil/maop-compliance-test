package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class Proceed extends Operation {

    private final @NotNull Entry @NotNull [] entries;

    public Proceed(@NotNull Entry @NotNull [] entries) {
        super((byte) 0x03);
        this.entries = entries;
    }

    public @NotNull Entry @NotNull [] getEntries() {
        return entries;
    }

    @Override
    public @NotNull String toString() {
        return "Proceed{" +
                "entries=" + Arrays.toString(entries) +
                '}';
    }

    @Override
    public byte @NotNull [] toBytes() {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(entries.length * 8);

        for (@NotNull Entry entry : entries) {
            buffer.putLong(entry.stream);
        }

        return buffer.array();
    }

    // Classes

    public static final class Entry {

        private final long stream;

        public Entry(long stream) {
            this.stream = stream;
        }

        public long getStream() {
            return stream;
        }

        @Override
        public @NotNull String toString() {
            return String.valueOf(stream);
        }
    }
}