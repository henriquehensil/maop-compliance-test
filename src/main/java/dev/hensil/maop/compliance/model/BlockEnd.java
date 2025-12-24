package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class BlockEnd extends Operation {

    private final long total;

    public BlockEnd(long total) {
        super((byte) 0x06);
        this.total = total;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(8)
                .putLong(total)
                .array();
    }
}
