package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.operation.Message;
import org.jetbrains.annotations.NotNull;

import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.log.NullLogger;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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

    public static void main(String[] args) throws Throwable {
        // Server
        @NotNull CountDownLatch latch = new CountDownLatch(1);
        @NotNull AtomicReference<QuicStream> reference = new AtomicReference<>(null);
        @NotNull ServerConnector server = newServer(3000, (ApplicationProtocolConnectionFactory) (s, quicConnection) -> new ApplicationProtocolConnection() {
            @Override
            public void acceptPeerInitiatedStream(QuicStream stream) {
                latch.countDown();
                reference.set(stream);
            }
        });

        server.start();

        //Start compliance
        @NotNull Preset preset = Preset.newBuilder()
                .uri(URI.create("https://localhost:3000"))
                .authenticationType("Basic")
                .vendor("Tester")
                .build();

        @NotNull Compliance compliance = new Compliance(preset);

        @NotNull Connection connection = compliance.createConnection("Test");
        @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

        byte @NotNull [] firstBytesLen = "Oi".getBytes();
        byte @NotNull [] secondBytesLen = "SALVEEEEEEEEEEEE".getBytes();
        stream.write(firstBytesLen);
        stream.write(secondBytesLen);

        // Retrieve the quic stream on the server side
        latch.await();
        @NotNull QuicStream quicStream = reference.get();

        byte @NotNull [] bytes = new byte[100];
        int read = quicStream.getInputStream().read(bytes);
        if (read > firstBytesLen.length) {
            System.out.println("MDSSSS");
        }

        if (read == firstBytesLen.length) {
            System.out.println("Ufa");
        }

        System.out.println(Arrays.toString(bytes));
        System.out.println("Datagram 1: " + firstBytesLen.length);
        System.out.println("Datagram 2: " + secondBytesLen.length);
        System.out.println("Read: " + read);
    }
}