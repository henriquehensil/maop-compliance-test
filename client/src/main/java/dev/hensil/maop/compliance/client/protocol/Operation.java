package dev.hensil.maop.compliance.client.protocol;

import org.jetbrains.annotations.NotNull;

public enum Operation {

    // Static enums

    MESSAGE((byte) 0x00, 11),
    REQUEST((byte) 0x01, 17),
    RESPONSE((byte) 0x02, 20),
    PROCEED((byte) 0x03, 2),
    REFUSE((byte) 0x04, 2),
    BLOCK((byte) 0x05, 4),
    BLOCK_END((byte) 0x06, 8),
    FAIL((byte) 0x07, 12),
    DONE((byte) 0x08, 2),
    DISCONNECT_REQUEST((byte) 0x09, 6),
    DISCONNECT((byte) 0x0A, 0)
    ;

    // Static initializers

    public static @NotNull Operation getByCode(byte code) {
        for (@NotNull Operation type : values()) {
            if (type.getCode() == code) {
                return type;
            }
        }

        throw new IllegalArgumentException("there's no operation type: " + code);
    }

    // Object

    private final byte code;
    private final int headerLength;

    Operation(byte code, int headerLength) {
        this.code = code;
        this.headerLength = headerLength;
    }

    // Getters

    public byte getCode() {
        return code;
    }

    public int getHeaderLength() {
        return headerLength;
    }

    public boolean isGlobalOperation() {
        return this == PROCEED || this == REFUSE || this == DONE || this == FAIL || this == DISCONNECT || this == DISCONNECT_REQUEST;
    }
}