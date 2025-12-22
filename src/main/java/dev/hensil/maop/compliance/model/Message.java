package dev.hensil.maop.compliance.model;

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
}
