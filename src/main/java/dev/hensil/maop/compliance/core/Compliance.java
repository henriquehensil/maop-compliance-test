package dev.hensil.maop.compliance.core;

import com.jlogm.Logger;
import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;
import com.jlogm.utils.Coloured;

import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.PluginInfo;
import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;
import dev.meinicke.plugin.category.AbstractPluginCategory;

import dev.meinicke.plugin.exception.PluginInitializeException;
import dev.meinicke.plugin.main.Plugins;

import org.jetbrains.annotations.*;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.log.NullLogger;

import java.awt.*;
import java.io.IOException;
import java.time.Duration;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class has no responsibility, and it doesn't exhibit any behavior for third-party Executors, except when it is initiated with its own Executor.
 * Therefore, if the executor belongs to the class, at the end of diagnostics, the executor will have certain defined behaviors according to its needs,
 * such as shutdown or renewal of another Executor if there are further attempts to start diagnostics.
 * */
public class Compliance {

    // Static initializers

    private static final @NotNull Logger log = Logger.create(Compliance.class).formatter(Main.FORMATTER);
    static final @NotNull Set<Situation> situations = new LinkedHashSet<>();

    static {
        try {
            Plugins.initializeAll();
        } catch (PluginInitializeException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TestOnly
    @VisibleForTesting
    static void addSituation(@NotNull Situation situation) {
        situations.add(situation);
    }

    @TestOnly
    @VisibleForTesting
    static void removeSituation(@NotNull Situation situation) {
        situations.remove(situation);
    }

    // Objects

    private final @NotNull UUID uuid = UUID.randomUUID();

    private final @NotNull Preset preset;
    private final @NotNull Map<String, Connection> connections = new ConcurrentHashMap<>();

    private @NotNull Executor executor;
    private @NotNull CompletableFuture<Void> join = new CompletableFuture<>();

    private volatile boolean selfExecutor;
    private volatile boolean running;

    // Constructor

    public Compliance(@NotNull Preset preset) {
        this.preset = preset;
        this.selfExecutor = true;
        this.executor = newDefaultExecutor();
    }

    public Compliance(@NotNull Preset preset, @NotNull Executor executor) {
        this.preset = preset;
        this.selfExecutor = false;
        this.executor = executor;
    }

    // Getters

    public @NotNull UUID getId() {
        return uuid;
    }

    public boolean isRunning() {
        return running;
    }

    public synchronized void setExecutor(@NotNull Executor executor) {
        if (running) {
            throw new IllegalStateException("Compliance is running");
        }

        this.executor = executor;
        this.selfExecutor = false;
    }

    private @NotNull Executor newDefaultExecutor() {
        return Executors.newSingleThreadExecutor(r -> {
            @NotNull Thread thread = new Thread(r);

            thread.setUncaughtExceptionHandler((t, ex) -> {
                try (
                        @NotNull LogCtx.Scope logContext = LogCtx.builder()
                                .put("compliance id", uuid)
                                .put("running", running)
                                .put("exception", ex)
                                .install();

                        @NotNull Stack.Scope logScope = Stack.pushScope("Uncaught exception handler");
                ) {
                    if (running) {
                        log.severe("Unexpected internal error");
                        log.debug().cause(ex).log();
                        stop();

                        return;
                    }

                    log.trace().cause(ex).log("Unexpected internal error occurs while stopping: " + ex);
                }
            });

            return thread;
        });
    }

    @ApiStatus.Internal
    void remove(@NotNull Connection connection) {
        for (@NotNull Map.Entry<String, Connection> entry : connections.entrySet()) {
            if (entry.getValue() == connection) {
                this.connections.remove(entry.getKey());
            }
        }
    }

    public @NotNull Preset getPreset() {
        return preset;
    }

    @ApiStatus.Internal
    public @Nullable Connection getConnection(@NotNull String name) {
        return this.connections.get(name);
    }

    // Modules

    public @NotNull Connection createConnection(@NotNull String name, @NotNull Situation situation) throws ConnectionException {
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", uuid)
                        .install();
                @NotNull Stack.Scope logScope = Stack.pushScope("Create")
        ) {
            log.trace("Creating new connection from the " + situation);
            @NotNull Connection connection = createConnection(name);
            return connection;
        }
    }

    @NotNull Connection createConnection(@NotNull String name) throws ConnectionException {
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("running", running)
                        .put("connection label", name)
                        .install();
                @NotNull Stack.Scope logScope = Stack.pushScope("Create connection")
        ) {
            @Nullable Connection connection = getConnection(name);
            if (connection != null) {
                log.trace("Closing and removing connection to be replacing for another connection name: " + connection);
                this.connections.remove(name);

                try {
                    connection.close();
                } catch (IOException e) {
                    log.trace("Cannot close connection: " + e);
                }
            }

            @NotNull Duration timeout = Duration.ofSeconds(2);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("replaced", connection != null)
                            .put("host", preset.getHost())
                            .put("no certification", preset.isServerNoCertification())
                            .put("connect timeout", timeout)
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Handle quic connection")
            ) {
                @NotNull QuicClientConnection.Builder builder = QuicClientConnection.newBuilder()
                        .uri(preset.getHost())
                        .applicationProtocol("maop/1")
                        .connectTimeout(timeout)
                        .logger(new NullLogger());

                if (preset.isServerNoCertification()) {
                    builder
                            .noServerCertificateCheck();
                } else {
                    builder
                            .clientCertificate(preset.getCertificate())
                            .clientCertificateKey(preset.getPrivateKey())
                            .clientKeyManager(preset.getKeyStore())
                            .clientKey(preset.getKeyPassword());
                }

                try {
                    @NotNull QuicClientConnection client = builder.build();
                    log.trace("Connecting in server.. (" + client + ")");

                    client.connect();

                    if (!client.isConnected()) {
                        throw new IOException("Cannot connect for unknown reason");
                    }

                    connection = new Connection(client, this);
                    this.connections.put(name, connection);

                    return connection;
                } catch (IOException e) {
                    throw new ConnectionException(e);
                } catch (Throwable e) {
                    throw new AssertionError("Internal error", e);
                }
            }
        }
    }

    // Modules

    public synchronized void start() {
        if (running) {
            throw new IllegalStateException("Already running");
        }

        if (situations.isEmpty()) {
            throw new AssertionError("Internal error");
        }

        this.running = true;
        this.join = new CompletableFuture<>();

        log.info("Initializing compliance diagnostics...");

        if (selfExecutor && ((ExecutorService) this.executor).isShutdown()) {
            this.executor = newDefaultExecutor();
        }

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", uuid)
                        .put("total situations", situations.size())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Starting compliance")
        ) {
            // Prepare situations

            @NotNull Set<Situation> situations = new LinkedHashSet<>(Compliance.situations);
            @NotNull AtomicBoolean canceled = new AtomicBoolean(false);

            for (@NotNull Situation situation : situations) {
                if (canceled.get() || Thread.currentThread().isInterrupted() || !running) {
                    break;
                }

                this.executor.execute(() -> {
                    try (
                            @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                    .put("remaining situations", situations.size())
                                    .put("situation name", situation.getName())
                                    .install();

                            @NotNull Stack.Scope logScope2 = Stack.pushScope("Scheduling situation")
                    ) {
                        if (canceled.get() || !running) {
                            return;
                        }

                        situations.remove(situation);

                        log.info("Next situation: " + Coloured.of(situation.getName()).color(Color.CYAN).print());
                        boolean severe = situation.diagnostic(this);

                        if (severe && running) {
                            log.severe("The " + situation + " ended severely. Interrupting all diagnostics...");
                            canceled.set(true);
                            stop();
                            return;
                        }

                        if (situations.isEmpty()) {
                            log.info("All diagnoses have been successfully completed");
                            stop();
                        }
                    }
                });
            }
        }
    }

    /**
     * This method waits until {@link #isRunning()} returns false and is unrelated to terminating executors, which, especially
     * if they are third-party, may still be running.
     * */
    @Blocking
    public void join() {
        this.join.join();
    }

    @Blocking
    public void join(int timeout) throws TimeoutException {
        try {
            this.join.orTimeout(timeout, TimeUnit.MILLISECONDS);
            this.join.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof TimeoutException to) {
                throw to;
            }

            throw new AssertionError("Internal error");
        }
    }

    public void stop() {
        if (!running) {
            return;
        }

        log.warn("Closing compliance");

        this.running = false;

        for (@NotNull String key : this.connections.keySet()) {
            try {
                @NotNull Connection connection = this.connections.remove(key);
                connection.close();
            } catch (IOException e) {
                log.trace("Cannot close connection while Compliance is stopping: " + e.getMessage());
            }
        }

        if (selfExecutor) {
            ((ExecutorService) this.executor).shutdownNow();
        }

        log.info("Successfully close compliance");

        this.join.complete(null);
    }

    // Classes

    @Plugin
    @Category("Category reference")
    private static final class PluginCategorySituation extends AbstractPluginCategory {

        private PluginCategorySituation() {
            super("Situation");
        }

        @Override
        public void run(@NotNull PluginInfo info) {
            @Nullable Object obj = info.getInstance();
            if (!(obj instanceof Situation situation)) {
                throw new AssertionError("Internal error: Cannot load all situations correctly");
            }

            boolean success = situations.add(situation);
            if (!success) {
                throw new AssertionError("Internal error");
            }
        }
    }
}