package dev.hensil.maop.compliance.model.operation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public final class Response extends Operation {

    private final byte @Nullable [] bytes;
    private final long payload;
    private final long start;
    private final int end;

    public Response(long payload, long start, int end) {
        super((byte) 0x02);
        this.payload = payload;
        this.start = start;
        this.end = end;
        this.bytes = null;
    }

    public long getPayload() {
        return payload;
    }

    public long getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(8 + 8 + 4)
                .putLong(8)
                .putInt(4)
                .array();
    }
}