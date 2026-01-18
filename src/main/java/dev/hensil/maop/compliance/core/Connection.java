package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;
import com.jlogm.utils.Coloured;

import dev.hensil.maop.compliance.Elapsed;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Approved;
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

    private static final @NotNull Logger log = Logger.create("Connection").formatter(Main.FORMATTER);
    private static final int GLOBAL_STREAM_LIMIT = 2;
    private static final int SEVERE_BEHAVIORS_LIMITS = 3;

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

    private final @NotNull AtomicInteger severeBehaviorCount = new AtomicInteger(0);
    private final @NotNull CompletableFuture<Void> polices = new CompletableFuture<>();
    private final @NotNull CountDownLatch disconnectionLatch = new CountDownLatch(1);

    private @Nullable Approved authentication = null;
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
        connection.setConnectionListener(event -> {
            log.warn("Connection terminate with reason: " + event.closeReason() + " (" + this + ")");
            try {
                this.close();
            } catch (IOException ignore) {
            }

            this.disconnectionLatch.countDown();
        });
    }

    // Getters

    public boolean isAuthenticated() {
        return isConnected() && authentication != null;
    }

    public void setAuthenticated(@NotNull Approved authentication) {
        if (closing) {
            return;
        }

        this.authentication = authentication;
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

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Internal authentication")
        ) {
            log.trace(Coloured.of("Authenticating connection (" + this + ")").color(Color.orange).print());

            @NotNull Authentication authentication = new Authentication(compliance.getPreset());
            @NotNull BidirectionalStream stream = createBidirectionalStream();

            @NotNull ByteBuffer bb = authentication.toByteBuffer();
            stream.write(bb.array(), bb.position(), bb.limit());
            stream.closeOutput();

            try {
                log.debug("Written authentication and waiting for Result response");
                @NotNull Result result = Result.readResult(stream, 5, TimeUnit.SECONDS);
                if (result instanceof Disapproved disapproved) {
                    throw new IOException("Authentication disapproved: " + disapproved);
                }

                setAuthenticated((Approved) result);
                log.trace(Coloured.of("Successfully authenticate connection (" + this + ")").color(Color.orange).print());

            } finally {
                try {
                    stream.closeInput();
                } catch (IOException ignore) {}

                boolean removed = this.streams.get(BidirectionalStream.class).remove(stream);
                if (!removed) {
                    log.warn("Bidirectional stream used in authentication has not been removed from the streams list.");
                }
            }
        }
    }

    public @NotNull UnidirectionalOutputStream createUnidirectionalStream() throws DirectionalStreamException {
        @NotNull CompletableFuture<UnidirectionalOutputStream> future = new CompletableFuture<>();
        future.orTimeout(8, TimeUnit.SECONDS);

        CompletableFuture.runAsync(() -> {
            try {
                future.complete(new UnidirectionalOutputStream(this, this.connection.createStream(false)));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Create")
        ) {
            log.trace(Coloured.of("Creating unidirectional output stream from connection (" + this + ")").color(Color.orange).print());

            @NotNull Elapsed elapsed = new Elapsed();
            @NotNull UnidirectionalOutputStream stream = future.join();
            elapsed.freeze();

            if (elapsed.getElapsedMillis() > 700) {
                log.warn("The server takes " + elapsed + " to create a unidirectional stream");
            }

            this.streams.get(UnidirectionalOutputStream.class).add(stream);
            log.trace("New unidirectional stream created by connection (" + this + ") with id: " + stream.getId());

            log.debug("Put stream (" + stream.getId() + ") as observable");
            observe(stream);

            return stream;
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
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

        CompletableFuture.runAsync(() -> {
            try {
                future.complete(new BidirectionalStream(this, this.connection.createStream(true)));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Create")
        ) {
            log.trace(Coloured.of("Creating bidirectional from connection (" + this + ")").color(Color.orange).print());

            @NotNull Elapsed elapsed = new Elapsed();
            @NotNull BidirectionalStream stream = future.join();
            elapsed.freeze();

            if (elapsed.getElapsedMillis() > 700) {
                log.warn("The server takes " + elapsed + " to create a bidirectional stream");
            }

            this.streams.get(BidirectionalStream.class).add(stream);
            log.trace("New bidirectional stream created by connection (" + this + ") with id: " + stream.getId());

            log.debug("Put stream (" + stream.getId() + ") as observable");
            observe(stream);

            return stream;
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException) {
                throw new DirectionalStreamException("Bidirectional Stream creation timeout", e.getCause());
            }

            if (e.getCause() instanceof IOException) {
                throw new DirectionalStreamException("Cannot create bidirectional stream", e.getCause());
            }

            throw new AssertionError("Internal error", e.getCause());
        }
    }

    private void observe(@NotNull DirectionalStream stream) {
        @Nullable DirectionalStreamObserver observer = this.observers.get(stream.getId());
        if (observer != null) {
            return;
        }

        observer = new DirectionalStreamObserver(stream);
        this.observers.put(stream.getId(), observer);
    }

    public boolean awaitDisconnection(int timeout, @NotNull TimeUnit unit) {
        try {
            return disconnectionLatch.await(timeout, unit);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Blocking
    public @NotNull Operation awaitOperation(@NotNull UnidirectionalOutputStream stream, int timeout, @NotNull TimeUnit timeUnit) throws IOException, TimeoutException {
        return await0(stream, timeout, timeUnit);
    }

    @Blocking
    public @NotNull Operation awaitOperation(@NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit timeUnit) throws IOException, TimeoutException {
        return await0(stream, timeout, timeUnit);
    }

    @Blocking
    public long awaitReading(@NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit unit) throws TimeoutException {
        return awaitReading(0, stream, timeout, unit);
    }

    @Blocking
    public long awaitReading(long untilAvailable, @NotNull BidirectionalStream stream, int timeout, @NotNull TimeUnit unit) throws TimeoutException {
        @Nullable DirectionalStreamObserver observer = null;

        try {
            long available = stream.available();
            if (available >= untilAvailable) {
                return available;
            }

            observer = this.getObserver(stream);
            if (observer == null) {
                throw new AssertionError("Internal error");
            }

            observer.setUntilAvailable(untilAvailable);

            boolean success = observer.awaitReading(timeout, unit);
            if (!success) {
                throw new TimeoutException(timeout + " " + unit.name().toLowerCase());
            }

            available = stream.available();
            if (available == 0) {
                throw new AssertionError("Internal error");
            }

            return available;
        } catch (IOException | IllegalStateException e) {
            throw new AssertionError("Internal error", e);
        }
    }

    @Blocking
    private @NotNull Operation await0(@NotNull DirectionalStream stream, int timeout, @NotNull TimeUnit timeUnit) throws TimeoutException {
        @Nullable DirectionalStreamObserver observer = this.observers.get(stream.getId());
        if (observer == null) {
            throw new AssertionError("Internal error");
        }

        @Nullable Operation operation = observer.awaitOperation(timeout, timeUnit);
        if (operation == null) {
            throw new TimeoutException(timeout + " " + timeUnit.name().toLowerCase());
        }

        return operation;
    }

    /**
     * Help method to increase counter and throws when polices limits are be exceeded.
     * */
    void reportGlobalPolicies() {
        this.severeBehaviorCount.incrementAndGet();
        int size = globalStreamSize();

        try (
                @NotNull LogCtx.Scope logCtx = LogCtx.builder()
                        .put("severe behaviour count", severeBehaviorCount.get())
                        .put("exceeded", isLimitExceeded())
                        .put("global stream size", size)
                        .install();

                @NotNull Stack.Scope scope = Stack.pushScope("Policies")
        ) {
            log.debug("Severe behaviour count count by connection ( " + this + " ) : " + severeBehaviorCount.get());

            if (isLimitExceeded()) {
                @NotNull Throwable throwable = new Throwable(size > GLOBAL_STREAM_LIMIT ? "Global stream limit exceeded: " + size : " Severe behaviors exceeded: " + severeBehaviorCount);
                this.polices.completeExceptionally(throwable);
            }
        }
    }

    private int globalStreamSize() {
        return streams.get(BidirectionalStream.class).stream()
                .filter(s -> s instanceof GlobalStream)
                .toList()
                .size();
    }

    private boolean isLimitExceeded() {
        return severeBehaviorCount.get() >= SEVERE_BEHAVIORS_LIMITS || globalStreamSize() >= GLOBAL_STREAM_LIMIT;
    }

    @Override
    public void close() throws IOException {
        if (closing) return;

        this.closing = true;
        this.authentication = null;

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
        return authentication != null ? authentication.getIdentifier() : connection.toString();
    }
}