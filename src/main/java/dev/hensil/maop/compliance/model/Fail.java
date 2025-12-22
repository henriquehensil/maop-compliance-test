package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

public final class Fail extends Operation {

    private final long stream;
    private final short error;
    private final short reasonLen;
    private final byte @NotNull [] reason;

    public Fail(long stream, short error, @NotNull String reason) {
        this(stream, error, (short) reason.length(), reason.getBytes(StandardCharsets.UTF_8));
    }

    public Fail(long stream, short error, byte @NotNull [] reason) {
        this(stream, error, (short) reason.length, reason);
    }

    public Fail(long stream, short error, short reasonLen, byte @NotNull [] reason) {
        super((byte) 0x07);
        this.stream = stream;
        this.error = error;
        this.reasonLen = reasonLen;
        this.reason = reason;
    }

    public long getStream() {
        return stream;
    }

    public short getError() {
        return error;
    }

    public short getReasonLen() {
        return reasonLen;
    }

    public byte @NotNull [] getReason() {
        return reason;
    }
}