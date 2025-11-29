package dev.hensil.maop.compliance.client.connection;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public final class Connections implements Iterable<Connection> {

    // Static initializers

    private static final @NotNull Connections instance = new Connections();

    public static @NotNull Connections getInstance() {
        return instance;
    }

    // Objects

    private final @NotNull Map<@NotNull String, @NotNull Connection> connections = new HashMap<>();

    private Connections() {
    }

    // Modules

    public @NotNull Connection create(@NotNull String name) throws ConnectionException {
        @NotNull Connection connection = new Connection(Main.getHost());
        this.connections.put(name.toLowerCase(), connection);

        return connection;
    }

    public @NotNull Connection getOrCreate(@NotNull String name) throws ConnectionException {
        return getConnection(name).orElse(create(name));
    }

    // Getters

    public @NotNull Optional<Connection> getConnection(@NotNull String name) {
        return Optional.ofNullable(this.connections.get(name.toLowerCase()));
    }

    public @NotNull Optional<Connection> getConnection(@NotNull Predicate<Connection> predicate) {
        return this.connections.values().stream().filter(predicate).findFirst();
    }

    public int size() {
        return connections.size();
    }

    @Override
    public @NotNull Iterator<Connection> iterator() {
        return this.connections.values().iterator();
    }
}