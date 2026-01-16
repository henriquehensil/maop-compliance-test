package dev.hensil.maop.compliance.core;

import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Operation;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.kwik.core.QuicStream;
import tech.kwik.core.server.ServerConnector;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

final class ConnectionWaiterMain {

    @Test
    public void testOperationWaiter() throws Throwable {
        @NotNull ServerConnector connector = Connections.newServer(3000, (s, quicConnection) -> {
            try {
                @NotNull QuicStream global = quicConnection.createStream(true);
                @NotNull Fail fail = new Fail(2, MAOPError.UNKNOWN_ERROR.getCode(), "Testing");
                global.getOutputStream().write(fail.getCode());
                global.getOutputStream().write(fail.toBytes());
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        connector.start();

        @NotNull Preset preset = Preset.newBuilder()
                .uri(URI.create("http://localhost:3000"))
                .vendor("test")
                .build();

        @NotNull Connection connection = new Compliance(preset).createConnection("Test");
        @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();
        stream.write("Hello".getBytes());

        @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
        Assertions.assertInstanceOf(Fail.class, operation);
    }
}
