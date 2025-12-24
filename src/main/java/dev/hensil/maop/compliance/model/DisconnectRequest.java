package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class DisconnectRequest extends Operation {

    private final int drainTime;
    private final short reasonLen;
    private final @NotNull String reason;

    public DisconnectRequest(int drainTime, short reasonLen, @NotNull String reason) {
        super((byte) 0x09);
        this.drainTime = drainTime;
        this.reasonLen = reasonLen;
        this.reason = reason;
    }

    public DisconnectRequest(byte code, int drainTime, @NotNull String reason) {
        super(code);
        this.drainTime = drainTime;
        this.reasonLen = (short) reason.length();
        this.reason = reason;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(4 + 2 + reason.length())
                .putInt(drainTime)
                .putShort(reasonLen)
                .put(reason.getBytes(StandardCharsets.UTF_8))
                .array();
    }
}
