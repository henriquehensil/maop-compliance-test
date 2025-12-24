package dev.hensil.maop.compliance;

import com.jlogm.Logger;

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

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class has no responsibility, and it doesn't exhibit any behavior for third-party Executors, except when it is initiated with its own Executor.
 * Therefore, if the executor belongs to the class, at the end of diagnostics, the executor will have certain defined behaviors according to its needs,
 * such as shutdown or renewal of another Executor if there are further attempts to start diagnostics.
 * */
public class Compliance {

    // Static initializers

    private static final @NotNull Logger log = Logger.create(Compliance.class);
    private static final @NotNull Set<Situation> situations = new LinkedHashSet<>();

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

    // Objects

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

    public boolean isRunning() {
        return running;
    }

    @NotNull Executor getExecutor() {
        return executor;
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
                log.severe().log("Unexpected internal error: " + ex.getMessage());
                this.stop();
            });

            return thread;
        });
    }

    @ApiStatus.Internal
    public @Nullable Connection getConnection(@NotNull String name) {
        return this.connections.get(name);
    }

    @ApiStatus.Internal
    public @NotNull Connection createConnection(@NotNull String name) throws IOException {
        if (!isRunning()) {
            throw new IllegalStateException("Compliance tests is not running");
        } else if (this.connections.containsKey(name)) {
            throw new AssertionError("Internal error");
        }

        @NotNull QuicClientConnection.Builder builder = QuicClientConnection.newBuilder()
                .uri(preset.getHost())
                .applicationProtocol("maop/1")
                .connectTimeout(Duration.ofSeconds(2))
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
            client.connect();

            if (!client.isConnected()) {
                throw new IOException("Cannot connect for known reason");
            }

            @NotNull Connection connection = new Connection(client, this);
            this.connections.put(name, connection);
            return connection;
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new AssertionError("Internal error", e);
        }
    }

    protected @NotNull Runnable getSituationRunnable() {
        return () -> {
            @NotNull Iterator<Situation> iterator = situations.iterator();

            while (!Thread.currentThread().isInterrupted() || isRunning()) {
                if (!iterator.hasNext()) {
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    log.info("All diagnostics have been completed!");
                    break;
                }

                @NotNull Situation situation = iterator.next();
                log.info("Next diagnose: " + situation.getName());

                boolean severe = situation.diagnostic();
                if (severe) {
                    log.warn("The " + situation + " ended severely. Interrupting all diagnostics...");
                    break;
                }
            }

            if (Thread.currentThread().isInterrupted()) {
                log.warn("Diagnostics were stopped abruptly");
            }

            stop();
        };
    }

    // Modules

    public synchronized void start() {
        if (situations.isEmpty()) {
            throw new AssertionError("Internal error");
        }

        this.running = true;

        log.info("Initializing diagnostics...");

        if (selfExecutor && ((ExecutorService) this.executor).isShutdown()) {
            this.executor = newDefaultExecutor();
        }

        // Prepare situations
        for (@NotNull Situation situation : situations) {
            try {
                @NotNull Field field = situation.getClass().getSuperclass().getDeclaredField("compliance");
                field.setAccessible(true);
                field.set(situation, this);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new AssertionError("Internal error", e);
            }
        }

        this.executor.execute(getSituationRunnable());
        this.join = new CompletableFuture<>();
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
        log.warn("Stopping diagnostics...");

        this.running = false;

        for (@NotNull String key : this.connections.keySet()) {
            try {
                @NotNull Connection connection = this.connections.remove(key);
                log.debug("Close: " + connection);
                connection.close();
            } catch (IOException e) {
                log.trace("Cannot close connection while Compliance is stopping: " + e.getMessage());
            }
        }

        if (selfExecutor) {
            ((ExecutorService) this.executor).shutdown();
        }

        log.info("Successfully stopped diagnostics");

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