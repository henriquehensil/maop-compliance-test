package dev.hensil.maop.compliance.client.situation.block;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.*;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.Operation;
import dev.hensil.maop.compliance.client.protocol.authentication.Disapproved;
import dev.hensil.maop.compliance.client.protocol.authentication.Result;
import dev.hensil.maop.compliance.client.situation.Situation;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Envia block antes de Proceed ser escrito
 * */
public final class EarlyBlockSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "Early block";
    }

    @Override
    public boolean execute() {
        try {
            // Get connections
            Main.log.info("Trying to find an open connection");
            @NotNull Connection connection = Connections.getInstance().getOrCreate("normal");
            if (!connection.isAuthenticated()) {
                Main.log.warn("It appears there is no connection in progress.");
                Main.log.warn("Authenticating again...");

                Main.log.trace("Creating new bidirectional stream to authentication");
                @NotNull BidirectionalStream stream = connection.getPools().createBidirectional();
                Main.log.trace("Write authentication");
                stream.writeAuthentication(Main.newAuthentication());
                Main.log.trace("Closing output");
                stream.closeOutput();

                Main.log.debug("Prepare to reading result");
                @NotNull CompletableFuture<Result> future = new CompletableFuture<>();
                future.orTimeout(2, TimeUnit.SECONDS);
                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.trace("Reading authentication result");
                        Result result = stream.readResult();
                        Main.log.debug("Successfully read authentication result");
                        future.complete(result);
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });

                try {
                    @NotNull Result result = future.join();

                    if (!result.getVersion().equals(Main.getVersion())) {
                        throw new InvalidVersionException("The version is different: " + result.getVersion());
                    }

                    if (result instanceof Disapproved disapproved) {
                        Main.log.severe("Authentication disapproved");
                        Main.log.severe(disapproved.toString());
                        return false;
                    }

                    Main.log.info("Successfully authenticated");
                    connection.setAuthenticated(true);
                } catch (CompletionException e) {
                    @NotNull Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException to) {
                        Main.log.severe("The timeout occurred while reading the authentication result: " + to.getClass().getSimpleName());
                    } else if (cause instanceof BufferUnderflowException b) {
                        Main.log.severe("The number of bytes read was insufficient to generate a valid result: " + b);
                    } else if (cause instanceof InvalidVersionException version) {
                        Main.log.severe(version.getClass().getSimpleName() + ": " + version.getMessage());
                    } else if (cause instanceof IOException) {
                        Main.log.severe("Seems a error occurs while to read a result: " + cause);
                    }

                    return false;
                } catch (InvalidVersionException e) {
                    Main.log.severe(e.getClass().getSimpleName() + ": " + e.getMessage());
                    return false;
                }
            }

            Main.log.trace("Create a output stream to write");
            @NotNull UnidirectionalDataOutput output = connection.getPools().createUnidirectional();

            int payload = 100;
            Main.log.info("Writing MESSAGE followed by BLOCK and BLOCK_END without waiting for PROCEED");

            ByteBuffer buffer = ByteBuffer.allocate(
                    1 + Operation.MESSAGE.getHeaderLength() +
                            1 + Operation.BLOCK.getHeaderLength() + payload +
                            1 + Operation.BLOCK_END.getHeaderLength()
            );
            // MESSAGE
            buffer.put(Operation.MESSAGE.getCode())
                    .putShort((short) 1)
                    .putLong(payload)
                    .put((byte) 0);

            // BLOCK
            buffer.put(Operation.BLOCK.getCode())
                    .putInt(payload)
                    .put(new byte[payload]);

            // BLOCK END
            buffer.put(Operation.BLOCK_END.getCode())
                    .putLong(payload);

            buffer.flip();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            output.write(bytes);
            Main.log.info("Payload written successfully");

            Main.log.trace("Waiting for a global stream");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(2000);

            Main.log.trace("Preparing to read fail on the global stream");
            {
                @NotNull CompletableFuture<Fail> future = new CompletableFuture<>();
                future.orTimeout(2, TimeUnit.SECONDS);
                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.info("Reading from the global stream");
                        @NotNull Fail fail = global.readFail();
                        future.complete(fail);
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });

                try {
                    @NotNull Fail fail = future.join();

                    Main.log.info("Successfully read");
                    Main.log.info("Starting verifications");

                    @NotNull Set<Fail.Code> expectedErrors = new HashSet<>() {{
                        this.add(Fail.Code.UNAUTHORIZED);
                        this.add(Fail.Code.PROTOCOL_VIOLATION);
                    }};

                    Main.log.trace("Expected errors: " + expectedErrors);

                    if (fail.getTarget() != global.getId()) {
                        Main.log.severe("This connection received a \"Fail\" but with a different stream ID than it should be: " + fail.getTarget());
                        return false;
                    }

                    if (!expectedErrors.contains(fail.getCode())) {
                        Main.log.warn("The error code doesn't seem to be what was expected: " + fail.getCode());
                        Main.log.warn("Be careful with protocol contracts.");
                    }
                } catch (CompletionException e) {
                    if (e.getCause() instanceof IOException io) {
                        Main.log.severe("An error occurs while to trying read on global stream: " + io.getMessage());
                    } else if (e.getCause() instanceof TimeoutException to) {
                        Main.log.severe("The timeout to read on the global stream was expired: " + to);
                    } else if (e.getCause() instanceof IllegalArgumentException fail) {
                        Main.log.severe("Invalid error code: " + fail.getMessage());
                    } else {
                        Main.log.severe("A internal error occurs: " + e.getCause());
                    }

                    return false;
                }
            }

            Main.log.info("Closing output");
            output.close();
            return true;
        } catch (ConnectionException e) {
            Main.log.severe("Error while attempting to establish connection: " + e);
        } catch (PoolException e) {
            Main.log.severe("Error while attempting to create a new pool: " + e.getClass().getSimpleName());
            Main.log.severe(e.toString());
        } catch (IOException e) {
            Main.log.severe("Error while attempting to use connection: " + e.getMessage());
        } catch (TimeoutException e) {
            Main.log.severe("The timeout to wait a global stream was expire: " + e.getMessage());
        }

        return false;
    }
}
