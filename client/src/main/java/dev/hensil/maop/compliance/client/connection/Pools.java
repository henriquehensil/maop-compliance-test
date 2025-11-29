package dev.hensil.maop.compliance.client.connection;

import dev.hensil.maop.compliance.client.exception.PoolException;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

import tech.kwik.core.QuicStream;
import tech.kwik.core.StreamReadListener;
import tech.kwik.core.stream.QuicStreamImpl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public final class Pools {

    private final @NotNull Connection connection;
    private final @NotNull Set<UnidirectionalDataOutput> unidirectionalOutputs = new HashSet<>();
    private final @NotNull Set<UnidirectionalDataInput> unidirectionalInputs = new HashSet<>();
    private final @NotNull Set<BidirectionalStream> bidirectionalStreams = new HashSet<>();

    Pools(@NotNull Connection connection) {
        this.connection = connection;
    }

    // Modules

    public @NotNull UnidirectionalDataOutput createUnidirectional() throws PoolException {
        try {
            @NotNull QuicStream quicStream = this.connection.getQuicConnection().createStream(false);
            @NotNull Pools.UnidirectionalOutput unidirectionalOutput = new UnidirectionalOutput((QuicStreamImpl) quicStream);
            this.unidirectionalOutputs.add(unidirectionalOutput);

            return unidirectionalOutput;
        } catch (Throwable e) {
            throw new PoolException(e.getMessage(), e);
        }
    }

    public @NotNull BidirectionalStream createBidirectional() throws PoolException {
        try {
            @NotNull QuicStream quicStream = this.connection.getQuicConnection().createStream(true);
            @NotNull BidirectionalStream bidirectionalStream = new BidirectionalStream((QuicStreamImpl) quicStream);
            this.bidirectionalStreams.add(bidirectionalStream);

            return bidirectionalStream;
        } catch (Throwable e) {
            throw new PoolException(e.getMessage(), e);
        }
    }

    // Getters

    public @NotNull Connection getConnection() {
        return connection;
    }

    @NotNull StreamReadListener getReadListener() {
        return (quicStream, l) -> {
            @NotNull QuicStreamImpl impl = (QuicStreamImpl) quicStream;
            if (impl.isPeerInitiated()) {
                if (impl.isUnidirectional()) {
                    @NotNull UnidirectionalDataInput input = new UnidirectionalInput(impl);
                    this.unidirectionalInputs.add(input);
                }
            } else {
                if (impl.isUnidirectional()) {
                    @NotNull UnidirectionalDataOutput output = new UnidirectionalOutput(impl);
                    this.unidirectionalOutputs.add(output);
                } else {
                    @NotNull BidirectionalStream b = new BidirectionalStream(impl);
                    this.bidirectionalStreams.add(b);
                }
            }
        };
    }

    public @Unmodifiable @NotNull Collection<BidirectionalStream> getBidirectionalStreams() {
        return Collections.unmodifiableSet(bidirectionalStreams);
    }

    public @Unmodifiable @NotNull Collection<UnidirectionalDataOutput> getUnidirectionalOutputs() {
        return Collections.unmodifiableSet(unidirectionalOutputs);
    }

    public @Unmodifiable @NotNull Collection<UnidirectionalDataInput> getUnidirectionalInputs() {
        return Collections.unmodifiableSet(unidirectionalInputs);
    }

    @Blocking
    public @NotNull UnidirectionalDataInput waitGlobalStream(int timeoutMs) throws TimeoutException {
        if (timeoutMs < 0 ) {
            throw new IllegalArgumentException("Illegal timeout value: " + timeoutMs);
        }

        @NotNull CompletableFuture<UnidirectionalDataInput> future = new CompletableFuture<>();
        future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);

        int pause = timeoutMs == 0 ? 1000 : timeoutMs / 2;

        CompletableFuture.runAsync(() -> {
            while (true) {
                @Nullable UnidirectionalDataInput global = this.unidirectionalInputs.stream()
                        .filter(UnidirectionalDataInput::isGlobal)
                        .findFirst()
                        .orElse(null);

                if (global == null) {
                    LockSupport.parkNanos(Duration.ofSeconds(pause).toNanos());
                    continue;
                }

                future.complete(global);
                break;
            }
        });

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException to) {
                throw to;
            }

            throw new AssertionError("Internal error");
        }
    }

    // Classes

    private static final class UnidirectionalOutput implements UnidirectionalDataOutput {

        private final int id;
        private final @NotNull DataOutputStream output;
        private volatile boolean global;

        private UnidirectionalOutput(@NotNull QuicStreamImpl quicStream) {
            if (!quicStream.isUnidirectional() && !quicStream.isSelfInitiated()) {
                throw new IllegalArgumentException("The quick stream must to be unidirectional and self initialized");
            }

            this.output = new DataOutputStream(quicStream.getOutputStream());
            this.id = quicStream.getStreamId();
        }

        @Override
        public int getId() {
            return id;
        }

        public boolean isGlobal() {
            return global;
        }

        public void setGlobal(boolean global) {
            this.global = global;
        }

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

        @Override
        public void close() throws IOException {
            this.output.close();
        }
    }

    private static final class UnidirectionalInput implements UnidirectionalDataInput {

        private final int id;
        private final @NotNull DataInputStream input;
        private volatile boolean global;

        private UnidirectionalInput(@NotNull QuicStreamImpl stream) {
            if (!stream.isUnidirectional() && !stream.isPeerInitiated()) {
                throw new IllegalArgumentException("The quick input stream must to be unidirectional and peer initialized");

            }

            this.input = new DataInputStream(stream.getInputStream());
            this.id = stream.getStreamId();
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public boolean isGlobal() {
            return global;
        }

        @Override
        public void setGlobal(boolean isGlobal) {
            this.global = isGlobal;
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

        @Override
        public void close() throws IOException {

        }
    }
}