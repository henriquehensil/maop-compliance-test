package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicStream;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public final class UnidirectionalOutputStream extends DirectionalStream implements DataOutput {

    private final @NotNull DataOutputStream outputStream;

    // Constructors

    UnidirectionalOutputStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        super(connection, stream);
        this.outputStream = new DataOutputStream(stream.getOutputStream());
    }

    // Modules

    public void write(int b) throws IOException {
        this.outputStream.write(b);
    }

    public void write(byte @NotNull [] b) throws IOException {
        this.outputStream.write(b);
    }

    public void write(byte @NotNull [] b, int off, int len) throws IOException {
        this.outputStream.write(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        this.outputStream.writeBoolean(v);
    }

    public void writeByte(int v) throws IOException {
        this.outputStream.writeByte(v);
    }

    public void writeShort(int v) throws IOException {
        this.outputStream.writeShort(v);
    }

    public void writeChar(int v) throws IOException {
        this.outputStream.writeChar(v);
    }

    public void writeInt(int v) throws IOException {
        this.outputStream.writeInt(v);
    }

    public void writeLong(long v) throws IOException {
        this.outputStream.writeLong(v);
    }

    public void writeFloat(float v) throws IOException {
        this.outputStream.writeFloat(v);
    }

    public void writeDouble(double v) throws IOException {
        this.outputStream.writeDouble(v);
    }

    public void writeBytes(@NotNull String s) throws IOException {
        this.outputStream.writeBytes(s);
    }

    public void writeChars(@NotNull String s) throws IOException {
        this.outputStream.writeChars(s);
    }

    public void writeUTF(@NotNull String s) throws IOException {
        this.outputStream.writeUTF(s);
    }

    @Override
    public void close() throws IOException {
        this.outputStream.close();
    }

    // Native

    @Override
    public boolean equals(@Nullable Object o) {
        return super.equals(o) && o.getClass() == this.getClass();
    }
}