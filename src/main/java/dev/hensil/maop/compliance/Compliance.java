package dev.hensil.maop.compliance;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.situation.Situation;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import tech.kwik.core.QuicClientConnection;
import tech.kwik.core.log.NullLogger;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class Compliance {

    // Static initializers

    private static final @NotNull Logger log = Logger.create(Compliance.class);

    // Objects

    private final @NotNull Preset preset;
    private final @NotNull Set<Situation> situations = new LinkedHashSet<>();
    private final @NotNull Map<String, Connection> connections = new ConcurrentHashMap<>();

    private @NotNull Executor executor;
    private volatile boolean running;

    // Constructor

    public Compliance(@NotNull Preset preset) {
        this(preset, Executors.newCachedThreadPool());
    }

    public Compliance(@NotNull Preset preset, @NotNull Executor executor) {
        this.preset = preset;
        this.executor = executor;
    }

    // Getters

    public boolean isRunning() {
        return running;
    }

    @NotNull Executor getExecutor() {
        return executor;
    }

    public void setExecutor(@NotNull Executor executor) {
        if (isRunning()) {
            throw new IllegalStateException("Compliance is running");
        }

        this.executor = executor;
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
            @NotNull Iterator<Situation> iterator = this.situations.iterator();

            while (!Thread.currentThread().isInterrupted() || isRunning()) {
                if (!iterator.hasNext()) {
                    log.info("All diagnostics have been completed!");
                    break;
                }

                @NotNull Situation situation = iterator.next();
                log.info("Next diagnose: " + situation.getName());

                boolean severe = situation.diagnostic();
                if (severe) {
                    log.warn("Interrupting diagnostics...");
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

    public void start() {
        if (situations.isEmpty()) {
            throw new AssertionError("Internal error");
        }

        this.running = true;

        log.info("Starting diagnostics...");

        this.executor.execute(getSituationRunnable());
    }

    public void stop() {
        this.running = false;

        for (@NotNull String key : this.connections.keySet()) {
            try {
                @NotNull Connection connection = this.connections.remove(key);
                connection.close();
            } catch (IOException e) {
                log.trace("Cannot close this connection while Compliance is stopping: " + e.getMessage());
            }
        }
    }
}