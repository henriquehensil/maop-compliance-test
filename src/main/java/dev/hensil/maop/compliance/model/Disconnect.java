package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

public final class Disconnect extends Operation {

    public Disconnect() {
        super((byte) 0x0A);
    }

    @Override
    public byte @NotNull [] toBytes() {
        return new byte[0];
    }
}