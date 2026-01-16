package dev.hensil.maop.compliance.model.operation;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.OperationUtil;
import dev.hensil.maop.compliance.model.SuccessMessage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Request extends Operation {

    // Static initializers

    public static @NotNull SuccessMessage writeRequest(@NotNull BidirectionalStream stream) throws TimeoutException, IOException {
        @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);

        stream.write(request.getCode());
        stream.write(request.toBytes());

        return SuccessMessage.readAfterRequest(stream);
    }

    // Objects

    private final short msgId;
    private final short responseId;
    private final long payload;
    private final byte priority;
    private final int timeout;

    public Request(short msgId, short responseId, long payload, byte priority, int timeout) {
        super((byte) 0x01);
        this.msgId = msgId;
        this.responseId = responseId;
        this.payload = payload;
        this.priority = priority;
        this.timeout = timeout;
    }

    public short getMsgId() {
        return msgId;
    }

    public short getResponseId() {
        return responseId;
    }

    public long getPayload() {
        return payload;
    }

    public byte getPriority() {
        return priority;
    }

    public int getTimeout() {
        return timeout;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(2 + 2 + 8 + 1 + 4)
                .putShort(msgId)
                .putShort(responseId)
                .putLong(payload)
                .put(priority)
                .putInt(timeout)
                .array();
    }
}