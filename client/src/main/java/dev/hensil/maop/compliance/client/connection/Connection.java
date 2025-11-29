package dev.hensil.maop.compliance.client.connection;

import codes.laivy.address.host.Host;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import org.jetbrains.annotations.NotNull;
import tech.kwik.core.QuicClientConnection;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

public final class Connection implements Closeable {

    private final @NotNull QuicClientConnection quicConnection;
    private final @NotNull Pools pools = new Pools(this);
    private volatile boolean authenticated = false;

    Connection(@NotNull Host host) throws ConnectionException {
        try {
            @NotNull QuicClientConnection connection = QuicClientConnection.newBuilder()
                    .applicationProtocol("maop/1")
                    .host(host.toString())
                    .build();

            connection.connect();

            if (!connection.isConnected()) {
                throw new IOException("failure when quic connection try to connect in the server");
            }

            connection.setStreamReadListener(pools.getReadListener());

            this.quicConnection = connection;
        } catch (IOException e) {
            throw new ConnectionException(e.getMessage(), e);
        }
    }

    // Getters

    public @NotNull Pools getPools() {
        return pools;
    }

    public boolean isAuthenticated() {
        return !isClosed() && authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        if (isClosed()) {
            throw new IllegalStateException("Connection is closed");
        }

        this.authenticated = authenticated;
    }

    @NotNull QuicClientConnection getQuicConnection() {
        return quicConnection;
    }

    public boolean isClosed() {
        return !quicConnection.isConnected();
    }

    @Override
    public void close() throws IOException {
        this.quicConnection.closeAndWait(Duration.ofSeconds(3));
    }
}