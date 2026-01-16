package dev.hensil.maop.compliance.model.operation;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Block extends Operation {

    private final int payload;
    private final byte @NotNull [] bytes;

    public Block(byte @NotNull [] bytes) {
        this(bytes.length, bytes);
    }

    public Block(int payload, byte @NotNull [] bytes) {
        super((byte) 0x05);
        this.payload = payload;
        this.bytes = bytes;
    }

    public long getPayload() {
        return payload;
    }

    public byte @NotNull [] getBytes() {
        return bytes;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(8 + bytes.length)
                .putLong(payload)
                .put(bytes)
                .array();
    }
}