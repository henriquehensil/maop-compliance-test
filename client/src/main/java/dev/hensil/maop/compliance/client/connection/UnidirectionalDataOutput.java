package dev.hensil.maop.compliance.client.connection;

import dev.hensil.maop.compliance.client.protocol.authentication.Authentication;
import dev.hensil.maop.compliance.client.protocol.Operation;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface UnidirectionalDataOutput extends DirectionalStream, DataOutput, Closeable {

    default void writeAuthentication(@NotNull Authentication authentication) throws IOException {
        @NotNull ByteBuffer buffer = authentication.toByteBuffer();
        write(buffer.array(), buffer.position(), buffer.limit());
    }

    default void writeRequest(short msgId, long payload, byte priority, short responseId, int timeout) throws IOException {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.REQUEST.getHeaderLength());
        buffer.put(Operation.REQUEST.getCode());
        // Message info
        buffer.putShort(msgId);
        buffer.putLong(payload);
        // Priority
        buffer.put(priority);
        // Response type and timeout
        buffer.putShort(responseId);
        buffer.putInt(timeout);
        // Flips
        buffer.flip();
        // Write
        write(buffer.array(), buffer.position(), buffer.limit());
    }

    default void writeMessage(short msgId, long payload, byte priority) throws IOException {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.MESSAGE.getHeaderLength());
        buffer.put(Operation.MESSAGE.getCode());
        // Message info
        buffer.putShort(msgId);
        buffer.putLong(payload);
        // Priority
        buffer.put(priority);
        // Flips
        buffer.flip();
        // Write
        write(buffer.array(), buffer.position(), buffer.limit());
    }

    default void writeBlock(byte @NotNull [] b) throws IOException {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.BLOCK.getHeaderLength());
        buffer.put(Operation.BLOCK.getCode());
        buffer.putInt(b.length);
        buffer.put(b);

        buffer.flip();

        write(buffer.array(), buffer.position(), buffer.limit());
    }

    default void writeBlockEnd(long totalLength) throws IOException {
        @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.BLOCK_END.getHeaderLength());
        buffer.put(Operation.BLOCK_END.getCode());
        buffer.putLong(totalLength);

        buffer.flip();

        write(buffer.array(), buffer.position(), buffer.limit());
    }
}