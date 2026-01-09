package dev.hensil.maop.compliance;

import org.jetbrains.annotations.NotNull;

import tech.kwik.core.log.NullLogger;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

public final class Connections {

    // Static initializers

    private static final @NotNull KeyStore DEFAULT_KEY_STORE;

    static {
        try {
            @NotNull KeyStore keyStore = KeyStore.getInstance("JKS");
            try (@NotNull InputStream is = new FileInputStream("C:\\Users\\User\\server.jks")) {
                keyStore.load(is, "123456".toCharArray());
            }

            DEFAULT_KEY_STORE = keyStore;

        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static @NotNull ServerConnector newServer(int port) throws Throwable {
        return newServer(port, (ApplicationProtocolConnectionFactory) (s, quicConnection) -> null);
    }

    public static @NotNull ServerConnector newServer(int port, @NotNull ApplicationProtocolConnectionFactory factory) throws Throwable {
        @NotNull ServerConnector connector = ServerConnector.builder()
                .withLogger(new NullLogger())
                .withPort(port)
                .withKeyStore(DEFAULT_KEY_STORE, "servercert", "123456".toCharArray())
                .build();

        connector.registerApplicationProtocol("maop/1", factory);

        return connector;
    }

    // Objects

    private Connections() {
        throw new UnsupportedOperationException();
    }
}