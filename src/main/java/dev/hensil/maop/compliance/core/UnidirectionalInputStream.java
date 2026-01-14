package dev.hensil.maop.compliance.core;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicStream;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

public final class UnidirectionalInputStream extends DirectionalStream implements DataInput {

    private final @NotNull DataInputStream inputStream;

    // Constructors

    UnidirectionalInputStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        super(connection, stream);
        this.inputStream = new DataInputStream(stream.getInputStream());
    }

    // Modules

    public int available() throws IOException {
        return inputStream.available();
    }

    public int read(byte @NotNull [] b) throws IOException {
        return inputStream.read(b);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return inputStream.read(b, off, len);
    }

    public void readFully(byte @NotNull [] b) throws IOException {
        this.inputStream.readFully(b);
    }

    public void readFully(byte @NotNull [] b, int off, int len) throws IOException {
        this.inputStream.readFully(b, off, len);
    }

    public int skipBytes(int n) throws IOException {
        return this.inputStream.skipBytes(n);
    }

    public boolean readBoolean() throws IOException {
        return this.inputStream.readBoolean();
    }

    public byte readByte() throws IOException {
        return this.inputStream.readByte();
    }

    public int readUnsignedByte() throws IOException {
        return this.inputStream.readUnsignedByte();
    }

    public short readShort() throws IOException {
        return this.inputStream.readShort();
    }

    public int readUnsignedShort() throws IOException {
        return this.inputStream.readUnsignedShort();
    }

    public char readChar() throws IOException {
        return this.inputStream.readChar();
    }

    public int readInt() throws IOException {
        return this.inputStream.readInt();
    }

    public long readLong() throws IOException {
        return this.inputStream.readLong();
    }

    public float readFloat() throws IOException {
        return this.inputStream.readFloat();
    }

    public double readDouble() throws IOException {
        return this.inputStream.readDouble();
    }

    @Override
    public @NotNull String readLine() throws IOException {
        return this.inputStream.readUTF();
    }

    public @NotNull String readUTF() throws IOException {
        return this.inputStream.readUTF();
    }

    @Override
    public void close() throws IOException {
        super.close();
        inputStream.close();
    }

    // Native

    @Override
    public boolean equals(@Nullable Object o) {
        return super.equals(o) && o.getClass() == this.getClass();
    }
}