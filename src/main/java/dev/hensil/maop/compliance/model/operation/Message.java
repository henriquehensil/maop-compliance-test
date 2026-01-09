package dev.hensil.maop.compliance.model.operation;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Message extends Operation {

    private final short msgId;
    private final long payload;
    private final byte priority;

    public Message(short msgId, long payload, byte priority) {
        super((byte) 0x00);
        this.msgId = msgId;
        this.payload = payload;
        this.priority = priority;
    }

    public short getMsgId() {
        return msgId;
    }

    public long getPayload() {
        return payload;
    }

    public byte getPriority() {
        return priority;
    }

    @Override
    public byte @NotNull [] toBytes() {
        return ByteBuffer.allocate(2 + 8 + 1)
                .putShort(msgId)
                .putLong(payload)
                .put(priority)
                .array();
    }
}
