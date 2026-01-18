package dev.hensil.maop.compliance.core;

import org.jetbrains.annotations.MustBeInvokedByOverriders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicStream;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public abstract class DirectionalStream implements Closeable {

    private final @NotNull Connection connection;
    private final @NotNull QuicStream stream;

    // Constructor

    protected DirectionalStream(@NotNull Connection connection, @NotNull QuicStream stream) {
        this.connection = connection;
        this.stream = stream;
    }

    // Getters

    public final long getId() {
        return stream.getStreamId();
    }

    protected final @NotNull QuicStream getQuicStream() {
        return stream;
    }

    public final @NotNull Connection getConnection() {
        return connection;
    }

    @Override
    @MustBeInvokedByOverriders
    public void close() throws IOException {
        @Nullable Set<DirectionalStream> stream = this.connection.getStreams().get(this.getClass());
        if (stream != null) {
            stream.remove(this);
        }
    }

    // Native

    @Override
    public @NotNull String toString() {
        return stream.toString();
    }

    @Override
    @MustBeInvokedByOverriders
    public boolean equals(@Nullable Object o) {
        if (!(o instanceof DirectionalStream that)) return false;
        return this.stream == that.stream && this == o;
    }

    @Override
    @MustBeInvokedByOverriders
    public int hashCode() {
        return Objects.hash(stream);
    }
}