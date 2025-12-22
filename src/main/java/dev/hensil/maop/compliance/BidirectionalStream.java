package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;
import tech.kwik.core.QuicStream;

import java.io.*;

public final class BidirectionalStream extends DirectionalStream implements DataInput, DataOutput {

    private final @NotNull DataOutputStream output;
    private final @NotNull DataInputStream input;
    private volatile boolean global;

    // Constructors

    BidirectionalStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        this(connection, stream, false);
    }

    BidirectionalStream(@NotNull Connection connection, @NotNull QuicStream quicStream, boolean global) {
        super(connection, quicStream);

        if (!quicStream.isBidirectional()) {
            throw new IllegalArgumentException("The quick stream must to be bidirectional");
        }

        this.output = new DataOutputStream(quicStream.getOutputStream());
        this.input = new DataInputStream(quicStream.getInputStream());
    }

    // Getters

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean isGlobal) {
        if (global) {
            throw new IllegalStateException("Cannot reverse global stream");
        }

        this.global = isGlobal;
    }

    // Modules

    public void write(int b) throws IOException {
        output.write(b);
    }

    public void write(byte @NotNull [] b) throws IOException {
        output.write(b);
    }

    public void write(byte @NotNull [] b, int off, int len) throws IOException {
        output.write(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        output.writeBoolean(v);
    }

    public void writeByte(int v) throws IOException {
        output.writeByte(v);
    }

    public void writeShort(int v) throws IOException {
        output.writeShort(v);
    }

    public void writeChar(int v) throws IOException {
        output.writeChar(v);
    }

    public void writeInt(int v) throws IOException {
        output.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        output.writeLong(v);
    }

    public void writeFloat(float v) throws IOException {
        output.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        output.writeDouble(v);
    }

    public void writeBytes(@NotNull String s) throws IOException {
        output.writeBytes(s);
    }

    public void writeChars(@NotNull String s) throws IOException {
        output.writeChars(s);
    }

    public void writeUTF(@NotNull String s) throws IOException {
        output.writeUTF(s);
    }

    public int read(byte @NotNull [] b) throws IOException {
        return input.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return input.read(b, off, len);
    }

    public void readFully(byte @NotNull [] b) throws IOException {
        input.readFully(b);
    }

    public void readFully(byte @NotNull [] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    public byte readByte() throws IOException {
        return input.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return input.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return input.readChar();
    }

    public int readInt() throws IOException {
        return input.readInt();
    }

    public long readLong() throws IOException {
        return input.readLong();
    }

    public float readFloat() throws IOException {
        return input.readFloat();
    }

    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public @NotNull String readLine() throws IOException {
        return input.readUTF();
    }

    public @NotNull String readUTF() throws IOException {
        return input.readUTF();
    }

    public void closeOutput() throws IOException {
        this.output.close();
    }

    public void closeInput() throws IOException {
        this.input.close();
    }

    @Override
    public void close() throws IOException {
        closeOutput();
        closeInput();
    }
}
