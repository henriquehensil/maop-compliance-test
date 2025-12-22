package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract sealed class Operation
        permits Block, BlockEnd, Disconnect, DisconnectRequest, Done, Fail, Message, Proceed, Refuse, Request, Response {

    private final byte code;

    protected Operation(byte code) {
        if (code < 0x00) {
            throw new IllegalArgumentException("Illegal code value");
        }

        this.code = code;
    }

    // Getters

    public final byte getCode() {
        return code;
    }

    public abstract byte @NotNull [] toBytes();

    // Native

    @Override
    public final boolean equals(@Nullable Object o) {
        return super.equals(o);
    }

    @Override
    public final int hashCode() {
        return super.hashCode();
    }
}