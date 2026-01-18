package dev.hensil.maop.compliance.model.operation;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Response extends Operation {

    private final long payload;
    private final long start;
    private final int end;

    public Response(long payload, long start, int end) {
        super((byte) 0x02);
        this.payload = payload;
        this.start = start;
        this.end = end;
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
                .putLong(payload)
                .putLong(start)
                .putInt(end)
                .array();
    }
}