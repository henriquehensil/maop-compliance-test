package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

public final class Block extends Operation {

    private final long payload;
    private final byte @NotNull [] bytes;

    public Block(long payload, byte @NotNull [] bytes) {
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
}