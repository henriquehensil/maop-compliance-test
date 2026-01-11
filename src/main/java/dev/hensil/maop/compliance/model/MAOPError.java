package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class MAOPError {

    // Static initializers

    public static final @NotNull MAOPError INVALID_FORMAT = new MAOPError("INVALID_FORMAT", (short) 0);
    public static final @NotNull MAOPError UNAUTHORIZED = new MAOPError("UNAUTHORIZED", (short) 1);
    public static final @NotNull MAOPError CONTENT_TOO_LARGE = new MAOPError("CONTENT_TOO_LARGE", (short) 2);
    public static final @NotNull MAOPError INVALID_HEADER = new MAOPError("INVALID_HEADER", (short) 3);
    public static final @NotNull MAOPError PAYLOAD_LENGTH_MISMATCH = new MAOPError("PAYLOAD_LENGTH_MISMATCH", (short) 4);
    public static final @NotNull MAOPError TIMEOUT = new MAOPError("TIMEOUT", (short) 5);
    public static final @NotNull MAOPError DUPLICATE_OPERATION = new MAOPError("DUPLICATE_OPERATION", (short) 6);
    public static final @NotNull MAOPError ORDER_VIOLATION = new MAOPError("ORDER_VIOLATION  ", (short) 7);
    public static final @NotNull MAOPError ID_MISMATCH = new MAOPError("ID_MISMATCH", (short) 8);
    public static final @NotNull MAOPError PARSE = new MAOPError("PARSE", (short) 9);
    public static final @NotNull MAOPError ILLEGAL_STREAM = new MAOPError("ILLEGAL_STREAM", (short) 10);
    public static final @NotNull MAOPError CANCELLED = new MAOPError("CANCELLED", (short) 11);
    public static final @NotNull MAOPError PROTOCOL_VIOLATION = new MAOPError("PROTOCOL_VIOLATION", (short) 12);
    public static final @NotNull MAOPError UNSUPPORTED_TEST_TYPE = new MAOPError("UNSUPPORTED_TEST_TYPE", (short) 13);
    public static final @NotNull MAOPError CAPACITY_LIMIT = new MAOPError("CAPACITY_LIMIT", (short) 14);
    public static final @NotNull MAOPError POLICY_UNDEFINED_LENGTH = new MAOPError("POLICY_UNDEFINED_LENGTH", (short) 15);
    public static final @NotNull MAOPError INCOMPATIBLE_MESSAGE = new MAOPError("INCOMPATIBLE_MESSAGE", (short) 16);
    public static final @NotNull MAOPError EXECUTION_ERROR = new MAOPError("EXECUTION_ERROR", (short) 17);
    public static final @NotNull MAOPError RESPONSE_MISMATCH = new MAOPError("RESPONSE_MISMATCH", (short) 18);
    public static final @NotNull MAOPError RESPONSE_CONSUME = new MAOPError("RESPONSE_CONSUME", (short) 19);
    public static final @NotNull MAOPError UNKNOWN_ERROR = new MAOPError("UNKNOWN_ERROR", (short) 49);

    private static final @NotNull Map<Short, MAOPError> errors = new HashMap<>();

    static {
        for (@NotNull Field field : MAOPError.class.getFields()) {
            if (field.getDeclaringClass() == MAOPError.class && Modifier.isStatic(field.getModifiers())) {
                try {
                    @NotNull MAOPError error = (MAOPError) field.get(null);
                    errors.put(error.getCode(), error);
                } catch (Throwable e) {
                    throw new AssertionError("Internal error", e);
                }
            }
        }
    }

    public static @Nullable MAOPError get(short code) {
        return errors.get(code);
    }

    public static boolean contains(short code) {
        return errors.containsKey(code);
    }

    // Objects

    private final @NotNull String name;
    private final short code;

    private MAOPError(@NotNull String name, short code) {
        this.name = name;
        this.code = code;
    }

    public @NotNull String getName() {
        return name;
    }

    public short getCode() {
        return code;
    }

    @Override
    public @NotNull String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        MAOPError MAOPError = (MAOPError) o;
        return code == MAOPError.code;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(code);
    }
}
