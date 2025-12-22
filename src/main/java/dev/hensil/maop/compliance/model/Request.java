package dev.hensil.maop.compliance.model;

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
}