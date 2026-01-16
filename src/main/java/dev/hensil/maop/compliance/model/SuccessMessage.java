package dev.hensil.maop.compliance.model;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.OperationUtil;
import dev.hensil.maop.compliance.model.operation.BlockEnd;
import dev.hensil.maop.compliance.model.operation.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class SuccessMessage {

    // Static initializers

    public static @NotNull SuccessMessage readAfterRequest(@NotNull BidirectionalStream stream) throws IOException, TimeoutException {
        @NotNull Connection connection = stream.getConnection();
        int expectedBytes = OperationUtil.RESPONSE.getHeaderLength() + 1;
        connection.awaitReading(expectedBytes, stream, 2, TimeUnit.SECONDS);

        byte code = stream.readByte();
        @Nullable OperationUtil util = OperationUtil.getByCode(code);
        if (util == null) {
            throw new IOException("Operation code not found: " + code);
        }

        if (util != OperationUtil.RESPONSE) {
            throw new IOException("Response operation was expected but it was " + util.getName());
        }

        @NotNull Response response = (Response) util.read(stream);
        if (response.getPayload() == 0) {
            throw new IOException("Success message was expected but the response payload is empty");
        }

        if (response.getPayload() > Short.MAX_VALUE) {
            throw new IOException("The response payload is too long for a simple Success message");
        }

        expectedBytes = OperationUtil.BLOCK.getHeaderLength() + 2;
        connection.awaitReading(expectedBytes, stream, 2, TimeUnit.SECONDS);

        @NotNull ByteBuffer buffer = ByteBuffer.allocate((short) response.getPayload());

        while (buffer.remaining() > 0) {
            try {
                code = stream.readByte();
                @Nullable OperationUtil blockUtil = OperationUtil.getByCode(code);
                if (blockUtil == null) {
                    throw new IOException("There is not operation with code: " + code);
                }

                if (blockUtil == OperationUtil.BLOCK_END) {
                    throw new IOException("Block end was received before reading all payload data (remaining = " + buffer.remaining() + ")");
                }

                if (blockUtil != OperationUtil.BLOCK) {
                    throw new IOException("A Block operation was expected but it was " + code + " (" + util.getName() + ")");
                }

                int blockPayload = stream.readInt();
                if (blockPayload > response.getPayload()) {
                    throw new IOException("Block payload is greater than the declared response payload: (block payload = " + blockPayload + " & response payload = " + response.getPayload() + ")");
                }

                int total = buffer.remaining() + blockPayload;
                if (total > response.getPayload()) {
                    throw new IOException("The number of blocks operations exceed the declared response payload (response payload = " + response.getPayload() + " & total read = " + total + ")");
                }

                connection.awaitReading(blockPayload, stream, 2, TimeUnit.SECONDS);

                byte @NotNull [] bytes = new byte[blockPayload];
                stream.readFully(bytes);
                buffer.put(bytes);
            } catch (BufferOverflowException e) {
                throw new IOException("The number of blocks operations exceed the declared response payload (response payload = " + response.getPayload() + ")", e);
            }
        }

        buffer.flip();
        expectedBytes = OperationUtil.BLOCK_END.getHeaderLength() + 1;
        connection.awaitReading(expectedBytes, stream, 2, TimeUnit.SECONDS);

        code = stream.readByte();
        util = OperationUtil.getByCode(code);
        if (util == null) {
            throw new IOException("Operation code not found: " + code);
        }

        if (util != OperationUtil.BLOCK_END) {
            throw new IOException("Block end operation was expected but it was " + util.getName());
        }

        @NotNull BlockEnd end = (BlockEnd) util.read(stream);
        if (end.getTotal() != response.getPayload()) {
            throw new IOException("Block end total bytes mismatch (block end total bytes = " + end.getTotal() + " & response payload = " + response.getPayload() + ")");
        }

        @NotNull SuccessMessage message = SuccessMessage.parse(buffer.array());
        return message;
    }

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