package dev.hensil.maop.compliance.model.operation;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Request extends Operation {

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