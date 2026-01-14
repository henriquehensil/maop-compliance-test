package dev.hensil.maop.compliance.model;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class SuccessMessage {

    // Static initializers

    public static @NotNull SuccessMessage parse(byte @NotNull [] bytes) {
        try {
            @NotNull ByteBuffer buffer = ByteBuffer.wrap(bytes);

            byte len = buffer.get();
            byte @NotNull [] content = new byte[len];

            buffer.get(content);

            byte bodyLen = buffer.get();
            byte @NotNull [] body = new byte[bodyLen];

            buffer.get(body);

            if (buffer.remaining() > 0) {
                throw new IllegalArgumentException("Non-matching bytes");
            }

            return new SuccessMessage(len, content, body);
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("Insufficient bytes");
        }
    }

    public static final short MESSAGE_ID = (short) 0;

    // Objects

    private final byte contentTypeLen;
    private final byte @NotNull [] contentType;
    private final byte @NotNull [] body;

    public SuccessMessage(byte contentTypeLen, byte @NotNull [] contentType, byte @NotNull [] body) {
        this.contentTypeLen = contentTypeLen;
        this.contentType = contentType;
        this.body = body;
    }

    public byte getContentTypeLen() {
        return contentTypeLen;
    }

    public byte @NotNull [] getContentType() {
        return contentType;
    }

    public @NotNull String contentTypeToString() {
        return new String(contentType, StandardCharsets.US_ASCII);
    }

    public byte @NotNull [] getBody() {
        return body;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        @NotNull SuccessMessage that = (SuccessMessage) o;
        return contentTypeLen == that.contentTypeLen && Objects.deepEquals(contentType, that.contentType) && Objects.deepEquals(body, that.body);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contentTypeLen, Arrays.hashCode(contentType), Arrays.hashCode(body));
    }
}