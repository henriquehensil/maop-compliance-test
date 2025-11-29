package dev.hensil.maop.compliance.client.connection;

import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.Operation;
import dev.hensil.maop.compliance.client.protocol.authentication.Result;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public interface UnidirectionalDataInput extends DirectionalStream, DataInput, Closeable {

    default @NotNull Result readResult() throws IOException {
        // Read result
        byte @NotNull [] result = readResultLegacy();

        try {
            return Result.parse(ByteBuffer.wrap(result));
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage());
        } catch (BufferUnderflowException e) {
            throw new IOException("Unsufficient bytes: " + e.getMessage());
        } catch (InvalidVersionException e) {
            throw new IOException("Illegal version: " + e.getMessage());
        }
    }

    default byte @NotNull [] readResultLegacy() throws IOException {
        // Read result
        @NotNull ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        @NotNull DataOutputStream dataOutputStream = new DataOutputStream(arrayOutputStream);

        boolean approved = readBoolean();
        if (approved) {
            long session = readLong();
            byte identifierLen = readByte();
            byte @NotNull [] identifier = new byte[identifierLen];
            readFully(identifier);

            dataOutputStream.writeBoolean(true);
            dataOutputStream.writeLong(session);
            dataOutputStream.writeByte(identifierLen);
            dataOutputStream.write(identifier);
        } else {
            dataOutputStream.writeBoolean(false);
            short code = readShort();
            int ms = readInt();

            short reasonLen = readShort();
            byte @NotNull [] reason = new byte[reasonLen];

            dataOutputStream.writeShort(code);
            dataOutputStream.writeInt(ms);
            dataOutputStream.writeShort(reasonLen);
            dataOutputStream.write(reason);
        }

        byte versionLen = readByte();
        byte @NotNull [] version = new byte[versionLen];
        readFully(version);

        byte vendorLen = readByte();
        byte @NotNull [] vendor = new byte[vendorLen];
        readFully(vendor);

        dataOutputStream.writeByte(versionLen);
        dataOutputStream.write(version);

        dataOutputStream.writeByte(vendorLen);
        dataOutputStream.write(vendor);

        return arrayOutputStream.toByteArray();
    }

    default long @NotNull [] readProceed() throws IOException {
        byte type = readByte();
        if (type != Operation.PROCEED.getCode()) {
            throw new IOException("The type of operation read is different from the PROCEED code: " + type);
        }

        short count = readShort();
        if (count < 0) {
            throw new IOException("Invalid PROCEED count: " + count);
        }

        long[] streams = new long[count];
        for (int i = 0; i < count; i++) {
            streams[i] = readLong();
        }

        return streams;
    }

    /**
     * @throws IllegalArgumentException if Error code is invalid
     * */
    default @NotNull Fail readFail() throws IOException {
        byte type = readByte();
        if (type != Operation.FAIL.getCode()) {
            throw new IOException("The type of operation read is different from the FAIL code:" + type);
        }

        long target = readLong();
        short code = readShort();
        @NotNull String reason = readUTF();

        return new Fail(target, Fail.Code.getByCode(code), reason);
    }
}