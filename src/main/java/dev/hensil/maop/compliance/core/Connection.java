package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;

import com.jlogm.utils.Coloured;

import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.model.operation.Operation;

import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicClientConnection;

import java.awt.*;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class Connection implements Closeable {

    // Static initializers

    private static final @NotNull Logger log = Logger.create("Connection");
    private static final int GLOBAL_STREAM_LIMIT = 2;
    private static final int STREAM_CREATED_BY_PEER_LIMIT = 3;

    private static void shutdown(@NotNull Connection connection) {
        if (connection.isClosed()) {
            return;
        }

        log.warn("Shutting down connection: " + connection);

        try {
            connection.close();
        } catch (IOException e) {
            log.warn("Error while closing connection: " + e);
        }

        connection.getCompliance().stop();
    }

    // Objects

    private final @NotNull Compliance compliance;
    private final @NotNull QuicClientConnection connection;

    private final @NotNull Map<Long, DirectionalStreamObserver> observers = new ConcurrentHashMap<>();

    private final @NotNull Map<Class<? extends DirectionalStream>, Set<DirectionalStream>> streams = new ConcurrentHashMap<>(3, 1f) {{
        this.put(BidirectionalStream.class, ConcurrentHashMap.newKeySet(16));
        this.put(UnidirectionalOutputStream.class, ConcurrentHashMap.newKeySet(16));
    }};

    private final @NotNull AtomicInteger streamCreateCount = new AtomicInteger(0);
    private final @NotNull CompletableFuture<Void> polices = new CompletableFuture<>();

    private volatile boolean authenticated = false;
    private volatile boolean closing = false;

    // Constructor

    Connection(@NotNull QuicClientConnection connection, @NotNull Compliance compliance) {
        this.connection = connection;
        this.compliance = compliance;

        this.polices.whenComplete((v, error) -> {
            if (error == null) {
                log.severe("The limit polices was exceeded from connection \"" + this + "\" Preparing to stop diagnostics..");
            } else {
                log.severe("The limit polices was exceeded ( " + error.getMessage() + ") from connection \"" + this + "\" Preparing to stop diagnostics..");
            }

            try {
                this.close();
            } catch (IOException ignore) {

            }

            this.compliance.stop();
        });

        connection.setStreamReadListener(GlobalStream.newGlobalListener(this));
        connection.setConnectionListener(connectionTerminatedEvent -> {
            log.warn("Connection terminate: " + this);
            try {
                this.close();
            } catch (IOException ignore) {
            }
        });
    }

    // Getters

    public boolean isAuthenticated() {
        return isConnected() && authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        if (closing) {
            return;
        }

        if (this.authenticated) {
            throw new IllegalStateException("Connection already authenticated. Cannot reverse authenticated connection field");
        }

        this.authenticated = authenticated;
    }

    @NotNull Map<Class<? extends DirectionalStream>, Set<DirectionalStream>> getStreams() {
        return streams;
    }

    @Nullable DirectionalStreamObserver getObserver(@NotNull DirectionalStream stream) {
        return observers.get(stream.getId());
    }

    @Nullable DirectionalStreamObserver getObserver(long streamId) {
        return observers.get(streamId);
    }

    @NotNull Compliance getCompliance() {
        return compliance;
    }

    public boolean isClosed() {
        return closing;
    }

    public boolean isConnected() {
        return this.connection.isConnected();
    }

    @SuppressWarnings("unchecked")
    public <T extends DirectionalStream> @Nullable T getDirectionalStream(@NotNull Class<? extends T> type, long id) {
        return (T) streams.get(type).stream()
                .filter(s -> s.getId() == id)
                .findFirst()
                .orElse(null);
    }

    public @Nullable DirectionalStream getDirectionalStream(long id) {
        @NotNull Collection<Set<DirectionalStream>> sets = this.streams.values();
        for (@NotNull Set<DirectionalStream> streams : sets) {
            for (@NotNull DirectionalStream stream : streams) {
                if (stream.getId() == id) {
                    return stream;
                }
            }
        }

        return null;
    }

    // Modules

    public void authenticate() throws IOException, TimeoutException {
        // Todo retry_after
        if (isAuthenticated()) {
            return;
        }

        log.trace(Coloured.of("Authenticating connection (" + this + ")").color(Color.orange).print());

        @NotNull Authentication authentication = new Authentication(compliance.getPreset());
        @NotNull BidirectionalStream stream = createBidirectionalStream();

        @NotNull ByteBuffer bb = authentication.toByteBuffer();
        stream.write(bb.array(), bb.position(), bb.limit());
        stream.closeOutput();

        try {
            log.debug("Written authentication and waiting for Result response");
            long expectedBytes = 1 + (16 + 1 + 1) + (2 + 4 + 2 + 1) + 1 + 1 + 1 + 1;
            long availableBytes;
            do {
                availableBytes = this.awaitReading(stream, 2, TimeUnit.SECONDS);
            } while (availableBytes < expectedBytes);

            @NotNull Result result = Result.readResult(stream);
            if (result instanceof Disapproved disapproved) {
                throw new IOException("Authentication disapproved: " + disapproved);
            }

            setAuthenticated(true);
            log.trace(Coloured.of("Successfully authenticate connection (" + this + ")").color(Color.orange).print());
        } finally {
            try {
                stream.closeInput();
            } catch (IOException ignore) {}
        }
    }

    public @NotNull UnidirectionalOutputStream createUnidirectionalStream() throws DirectionalStreamException {
        @NotNull CompletableFuture<UnidirectionalOutputStream> future = new CompletableFuture<>();
        future.orTimeout(8, TimeUnit.SECONDS);

        @NotNull CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
            log.trace(Coloured.of("Creating unidirectional output stream from connection (" + this + ")").color(Color.orange).print());

            try {
                long ms = System.currentTimeMillis();
                @NotNull UnidirectionalOutputStream stream = new UnidirectionalOutputStream(this, this.connection.createStream(false));

                long timeExec = ms - System.currentTimeMillis();

                this.streams.get(UnidirectionalOutputStream.class).add(stream);
                log.trace(Coloured.of("New Unidirectional stream created by connection (" + this + ") with id: " + stream.getId()).color(Color.orange).print());
                if (timeExec > 1000) {
                    log.warn("The server takes " + timeExec + " ms to create a unidirectional stream");
                }

                future.complete(stream);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                task.cancel(true);
                throw new DirectionalStreamException("Unidirectional Stream creation timeout", e.getCause());
            }

            if (e.getCause() instanceof IOException) {
                throw new DirectionalStreamException("Cannot create unidirectional stream", e.getCause());
            }

            throw new AssertionError("Internal error", e.getCause());
        }
    }

    public @NotNull BidirectionalStream createBidirectionalStream() throws DirectionalStreamException {
        @NotNull CompletableFuture<BidirectionalStream> future = new CompletableFuture<>();
        future.orTimeout(8, TimeUnit.SECONDS);

        @NotNull CompletableFuture<Void> task = CompletableFuture.runAsync(() -> {
            log.debug("Creating Bidirectional output stream from connection (" + this + ")");

            try {
                long ms = System.currentTimeMillis();
                @NotNull BidirectionalStream stream = new BidirectionalStream(this, this.connection.createStream(true));

                long timeExec = ms - System.currentTimeMillis();

                this.streams.get(BidirectionalStream.class).add(stream);

                log.info(Coloured.of("New Bidirectional stream created by connection (" + this + ") with id: " + stream.getId()).color(Color.orange).print());
                if (timeExec > 1000) {
                    log.warn("The server takes " + timeExec + " ms to create a bidirectional stream");
                }

                future.complete(stream);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try {
            @NotNull BidirectionalStream stream = future.join();

            log.debug("Put stream (" + stream.getId() + ") as observable");

            observe(stream);

            return stream;
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                task.cancel(true);
                log.severe("Bidirectional Stream creation timeout");

                try {
                    close();
                } catch (IOException ex) {
                    log.trace("Close connection failed: " + ex);
                }

                log.debug("Stopping compliance by connection (" + this + ")");
                this.compliance.stop();
            }

            if (e.getCause() instanceof IOException) {
                throw new DirectionalStreamException("Cannot create bidirectional stream", e);
            }

            throw new AssertionError("Internal error");
        }
    }

    private @NotNull DirectionalStreamObserver observe(@NotNull DirectionalStream stream) {
        @Nullable DirectionalStreamObserver observer = this.observers.get(stream.getId());
        if (observer != null) {
            return observer;
        }

        observer = new DirectionalStreamObserver(stream);
        this.observers.put(stream.getId(), observer);
        return observer;
    }

    @Blocking
    public @NotNull Operation await(@NotNull UnidirectionalOutputStream stream, int timeout, @NotNull TimeUnit timeUnit) throws IOException, TimeoutException, InterruptedException {
        return await0(stream, timeout, timeUnit);
    }

    @Blocking
    public @NotNull Operation await(@NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit timeUnit) throws IOException, TimeoutException, InterruptedException {
        return await0(stream, timeout, timeUnit);
    }

    @Blocking
    public long awaitReading(@NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit unit) throws TimeoutException {
        try {
            long available = stream.available();
            if (available > 0) {
                return available;
            }

            @Nullable DirectionalStreamObserver observer = this.getObserver(stream);
            if (observer == null) {
                throw new AssertionError("Internal error");
            }

            if (!observer.isWaitReading()) {
                observer.setWaitReading(true);
            }

            boolean success = observer.awaitReading(timeout, unit);
            if (!success) {
                throw new TimeoutException();
            }

            available = stream.available();
            if (available == 0) {
                throw new AssertionError("Internal error");
            }

            observer.setWaitReading(false);

            return available;
        } catch (IOException e) {
            throw new AssertionError("Internal error", e);
        }
    }

    @Blocking
    private @NotNull Operation await0(@NotNull DirectionalStream stream, int timeout, @NotNull TimeUnit timeUnit) throws IOException, TimeoutException, InterruptedException {
        @Nullable DirectionalStreamObserver observer = this.observers.get(stream.getId());
        if (observer == null) {
            observer = observe(stream);
            log.trace("Non observable stream: " + stream);
        }

        return observer.awaitOperation(timeout, timeUnit);
    }

    /**
     * Help method to increase the stream counter and throws when polices limits are be exceeded.
     * */
    void reportGlobalPolices() {
        this.streamCreateCount.incrementAndGet();

        log.debug("Stream count by connection ( " + this + " ) : " + streamCreateCount.get());

        if (isLimitExceeded()) {
            this.polices.complete(null);
        }
    }

    private boolean isLimitExceeded() {
        return streamCreateCount.get() >= STREAM_CREATED_BY_PEER_LIMIT
                || streams.get(BidirectionalStream.class).size() >= GLOBAL_STREAM_LIMIT;
    }

    @Override
    public void close() throws IOException {
        if (closing) return;

        this.closing = true;
        this.authenticated = false;

        this.compliance.remove(this);

        for (@NotNull Set<DirectionalStream> set : streams.values()) {
            for (@NotNull DirectionalStream stream : set) {
                try {
                    this.observers.remove(stream.getId());
                    stream.close();
                } catch (IOException ignore) {}
            }
            set.clear();
        }

        this.connection.close();
    }

    @Override
    public @NotNull String toString() {
        return this.connection.toString();
    }
}