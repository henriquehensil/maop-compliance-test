package dev.hensil.maop.compliance;

import dev.hensil.maop.compliance.model.operation.*;
import dev.hensil.maop.compliance.situation.Situation;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.core.server.ServerConnector;

import java.io.IOException;
import java.net.URI;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

final class ComplianceSecurityTests {

    private static final @NotNull Preset SIMPLE_PRESET = Preset.newBuilder()
            .uri(URI.create("https://localhost:8080"))
            .build();

    // Objects

    @Test
    public void testGlobalManagerMessageOperation() throws Throwable {
        // Variables
        @NotNull Message message = new Message((short) 2, 0L, (byte) 0);
        @NotNull AtomicReference<Long> streamId = new AtomicReference<>(0L);

        @NotNull ApplicationProtocolConnectionFactory factory = (s, quicConnection) -> new ApplicationProtocolConnection() {
            @Override
            public void acceptPeerInitiatedStream(QuicStream stream) {
                try {
                    @NotNull QuicStream global = quicConnection.createStream(true);
                    @NotNull Done done = new Done(new Done.Entry[]{
                            new Done.Entry(streamId.get() , System.currentTimeMillis(), 300)
                    });

                    global.getOutputStream().write((byte) done.getCode());
                    global.getOutputStream().write(done.toBytes());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        @NotNull AtomicReference<Operation> reference = new AtomicReference<>(null);

        try (@NotNull ServerConnector server = Connections.newServer(8080, factory)) {
            server.start();

            @NotNull Compliance compliance = new Compliance(SIMPLE_PRESET);
            Compliance.addSituation(new Situation() {
                @Override
                public @NotNull String getName() {
                    return "Test Situation";
                }

                @Override
                public boolean diagnostic() {
                    try {
                        @NotNull Connection connection = getCompliance().createConnection("Test", this);
                        @NotNull UnidirectionalOutputStream output = connection.createUnidirectionalStream();

                        streamId.set(output.getId());

                        connection.manage(output, message);
                        output.write((byte) message.getCode());
                        output.write(message.toBytes());

                        @NotNull Operation operation = connection.await(message, 1000);
                        reference.set(operation);

                        Compliance.removeSituation(this);

                    } catch (InterruptedException ignore) {

                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }

                    return false;
                }
            });

            compliance.start();
            compliance.join(1000);

            @Nullable Operation operation = reference.get();
            Assertions.assertNotNull(operation);
            Assertions.assertInstanceOf(Done.class, operation);
        }
    }

    @Test
    public void testGlobalManagerRequestOperation() throws Throwable {
        // Variables
        @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);
        @NotNull AtomicReference<Long> streamId = new AtomicReference<>(0L);

        @NotNull ApplicationProtocolConnectionFactory factory = (s, quicConnection) -> new ApplicationProtocolConnection() {
            @Override
            public void acceptPeerInitiatedStream(QuicStream stream) {
                try {
                    if (!stream.isBidirectional()) {
                        throw new IOException("Not bidirectional");
                    }

                    @NotNull Response response = new Response(0L, System.currentTimeMillis(), 300);
                    stream.getOutputStream().write((byte) response.getCode());
                    stream.getOutputStream().write(response.toBytes());

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        @NotNull AtomicReference<Operation> reference = new AtomicReference<>(null);

        try (@NotNull ServerConnector server = Connections.newServer(8080, factory)) {
            server.start();

            @NotNull Compliance compliance = new Compliance(SIMPLE_PRESET);
            Compliance.addSituation(new Situation() {
                @Override
                public @NotNull String getName() {
                    return "Test Situation";
                }

                @Override
                public boolean diagnostic() {
                    try {
                        @NotNull Connection connection = getCompliance().createConnection("Test", this);
                        @NotNull BidirectionalStream bidirectionalStream = connection.createBidirectionalStream();

                        streamId.set(bidirectionalStream.getId());

                        connection.manage(bidirectionalStream, request);
                        bidirectionalStream.write((byte) request.getCode());
                        bidirectionalStream.write(request.toBytes());

                        @NotNull Operation operation = connection.await(request, 1000);
                        reference.set(operation);

                        Compliance.removeSituation(this);

                    } catch (InterruptedException ignore) {

                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }

                    return false;
                }
            });

            compliance.start();
            compliance.join(1000);

            @Nullable Operation operation = reference.get();
            Assertions.assertNotNull(operation);
            Assertions.assertInstanceOf(Response.class, operation);
        }
    }

    @Test
    @DisplayName("Test the stream create limit peer connection and if stop compliance")
    public void testSteamCreateLimit() throws Throwable {
        // Variables
        @NotNull CountDownLatch connectionLatch = new CountDownLatch(1);
        @NotNull CountDownLatch situationLatch = new CountDownLatch(1);
        @NotNull AtomicReference<QuicConnection> reference = new AtomicReference<>(null);

        @NotNull ApplicationProtocolConnectionFactory factory = (s, quicConnection) -> {
            reference.set(quicConnection);
            connectionLatch.countDown();
            return null;
        };

        // Open Server
        try (@NotNull ServerConnector server = Connections.newServer(8080, factory)) {
            server.start();

            @NotNull Compliance compliance = new Compliance(SIMPLE_PRESET, Executors.newSingleThreadExecutor());
            Compliance.addSituation(new Situation() {
                @Override
                public @NotNull String getName() {
                    return "Test Situation";
                }

                @Override
                public boolean diagnostic() {
                    try {
                        // Create connection
                        getCompliance().createConnection("Test", this);

                        situationLatch.await();
                    } catch (InterruptedException ignore) {

                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }

                    Compliance.removeSituation(this);
                    return false;
                }
            });

            // Starts
            compliance.start();

            connectionLatch.await();

            // Create various Unidirectional Stream
            @Nullable QuicConnection connection = reference.get();
            if (connection == null) {
                Assertions.fail("Connection reference is not defined");
            }

            for (int i = 0; i < Connection.STREAM_CREATED_BY_PEER_LIMIT; i++) {
                connection.createStream(false).getOutputStream().close();
            }


            compliance.join(1000);
            Assertions.assertFalse(compliance.isRunning());

            situationLatch.countDown();
        }
    }

    @Test
    public void testGlobalSteamCreateLimit() throws Throwable {
        // Variables
        @NotNull CountDownLatch connectionLatch = new CountDownLatch(1);
        @NotNull CountDownLatch situationLatch = new CountDownLatch(1);
        @NotNull AtomicReference<QuicConnection> reference = new AtomicReference<>(null);
        @NotNull ApplicationProtocolConnectionFactory factory = (s, quicConnection) -> {
            reference.set(quicConnection);
            connectionLatch.countDown();
            return null;
        };

        // Open Server
        try (@NotNull ServerConnector server = Connections.newServer(8080, factory)) {
            server.start();

            @NotNull Compliance compliance = new Compliance(SIMPLE_PRESET);
            Compliance.addSituation(new Situation() {
                @Override
                public @NotNull String getName() {
                    return "Test Situation";
                }

                @Override
                public boolean diagnostic() {
                    try {
                        // Create connection
                        getCompliance().createConnection("Test", this);
                        situationLatch.await();
                    } catch (InterruptedException ignore) {

                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }

                    Compliance.removeSituation(this);
                    return false;
                }
            });

            // Starts
            compliance.start();
            connectionLatch.await();

            // Create various Bidirectional Stream
            @Nullable QuicConnection connection = reference.get();
            if (connection == null) {
                Assertions.fail("Connection reference is not defined");
            }

            for (int i = 0; i < Connection.GLOBAL_STREAM_LIMIT; i++) {
                connection.createStream(true).getOutputStream().close();
            }

            compliance.join(1000);
            Assertions.assertFalse(compliance.isRunning());

            situationLatch.countDown();
        }
    }
}