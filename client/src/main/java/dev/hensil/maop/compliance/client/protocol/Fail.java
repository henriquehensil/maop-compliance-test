package dev.hensil.maop.compliance.client.protocol;

import org.jetbrains.annotations.NotNull;

public final class Fail {

    private final long target;
    private final @NotNull Code code;
    private final @NotNull String reason;

    public Fail(long target, @NotNull Code code, @NotNull String reason) {
        this.target = target;
        this.code = code;
        this.reason = reason;
    }

    public long getTarget() {
        return target;
    }

    public @NotNull Code getCode() {
        return code;
    }

    public @NotNull String getReason() {
        return reason;
    }

    @Override
    public @NotNull String toString() {
        return "Fail{" +
                "target=" + target +
                ", code=" + code +
                ", reason='" + reason + '\'' +
                '}';
    }

    // Classes

    public enum Code {

        // Static enums

        INVALID_FORMAT((short) 0),
        UNAUTHORIZED((short) 1),
        PAYLOAD_LENGTH_MISMATCH((short) 4),
        TIMEOUT((short) 5),
        ORDER_VIOLATION((short) 7),
        ILLEGAL_STREAM((short) 10),
        CANCELLED((short) 11),
        PROTOCOL_VIOLATION((short) 12),
        ;

        // Static initializers

        public static @NotNull Code getByCode(short code) {
            for (@NotNull Code type : values()) {
                if (type.getValue() == code) {
                    return type;
                }
            }

            throw new IllegalArgumentException("there's no code type: " + code);
        }

        // Objects

        private final short value;

        Code(short value) {
            this.value = value;
        }

        public short getValue() {
            return value;
        }
    }
}
