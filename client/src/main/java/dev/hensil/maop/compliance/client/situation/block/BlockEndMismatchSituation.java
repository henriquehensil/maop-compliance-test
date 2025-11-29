package dev.hensil.maop.compliance.client.situation.block;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.Connection;
import dev.hensil.maop.compliance.client.connection.Connections;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataInput;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataOutput;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.Operation;
import dev.hensil.maop.compliance.client.situation.Situation;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends a MESSAGE with payload_len = 100, waits for PROCEED and then sends a BLOCK_END
 * with total_length = 0 (no BLOCK frames sent).
 *
 */
public final class BlockEndMismatchSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "MESSAGE payload length mismatch (BLOCK_END with zero length)";
    }

    @Override
    public boolean execute() {
        try {
            // Get authenticated connection
            Main.log.info("Trying to obtain an authenticated connection");
            @NotNull Connection connection = Connections.getInstance().getOrCreate("normal");

            if (!connection.isAuthenticated()) {
                Main.log.severe("Connection is not authenticated. This situation requires a pre-authenticated connection.");
                return false;
            }

            Main.log.trace("Creating a unidirectional stream for MESSAGE and BLOCK_END");
            @NotNull UnidirectionalDataOutput output = connection.getPools().createUnidirectional();

            {
                long expectedPayloadLength = 100L;
                short msgId = 1;
                byte priority = 0;

                Main.log.info("Sending MESSAGE with payload_len = " + expectedPayloadLength);

                int capacity = 1 + Operation.MESSAGE.getHeaderLength();
                @NotNull ByteBuffer buffer = ByteBuffer.allocate(capacity);
                buffer.put(Operation.MESSAGE.getCode())
                        .putShort(msgId)
                        .putLong(expectedPayloadLength)
                        .put(priority);

                buffer.flip();
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);

                output.write(bytes);
                Main.log.info("MESSAGE frame written successfully");
            }

            Main.log.info("Waiting for PROCEED frame on a global control stream");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(2000);

            {
                CompletableFuture<long[]> proceedFuture = new CompletableFuture<>();
                proceedFuture.orTimeout(2, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.trace("Reading PROCEED from global stream " + global.getId());
                        long[] allowedStreams = global.readProceed();
                        Main.log.info("Successfully read PROCEED");
                        proceedFuture.complete(allowedStreams);
                    } catch (Throwable e) {
                        proceedFuture.completeExceptionally(e);
                    }
                });

                try {
                    long[] allowedStreams = proceedFuture.join();
                    Main.log.trace("Streams allowed by PROCEED: " + Arrays.toString(allowedStreams));

                    if (allowedStreams.length == 0) {
                        Main.log.severe("PROCEED did not include any allowed streams");
                        return false;
                    }

                    boolean contains = false;
                    for (long id : allowedStreams) {
                        if (id == output.getId()) {
                            contains = true;
                            break;
                        }
                    }

                    if (!contains) {
                        Main.log.severe("PROCEED does not include the ID used by the output to write.");
                        return false;
                    }

                } catch (CompletionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException to) {
                        Main.log.severe("Timeout while waiting for PROCEED on global stream: " + to);
                    } else if (cause instanceof IOException io) {
                        Main.log.severe("I/O error while reading PROCEED on global stream: " + io.getMessage());
                    } else if (cause instanceof IllegalArgumentException fail) {
                        Main.log.severe("Invalid PROCEED frame: " + fail.getMessage());
                    } else {
                        Main.log.severe("An internal error occurred while reading PROCEED: " + cause);
                    }
                    return false;
                }
            }

            Main.log.info("Sending BLOCK_END with total_length = 0 (no BLOCK frames sent)");
            {
                long totalLength = 0L;

                ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.BLOCK_END.getHeaderLength());
                buffer.put(Operation.BLOCK_END.getCode())
                        .putLong(totalLength);

                buffer.flip();

                output.write(buffer.array(), buffer.position(), buffer.limit());
                Main.log.info("BLOCK_END frame written successfully");
            }

            Main.log.info("Waiting for FAIL frame on a global control stream as a reaction to mismatched payload length");

            {
                CompletableFuture<Fail> future = new CompletableFuture<>();
                future.orTimeout(2, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.info("Reading FAIL from global stream " + global.getId());
                        @NotNull Fail fail = global.readFail();
                        future.complete(fail);
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });

                try {
                    @NotNull Fail fail = future.join();

                    Main.log.info("FAIL frame successfully read");
                    Main.log.info("Starting validations");

                    Set<Fail.Code> expectedCodes = new HashSet<>() {{
                        add(Fail.Code.PAYLOAD_LENGTH_MISMATCH);
                        add(Fail.Code.INVALID_FORMAT);
                        add(Fail.Code.PROTOCOL_VIOLATION);
                    }};

                    Main.log.trace("Accepted FAIL codes for this situation: " + expectedCodes);

                    if (fail.getTarget() != output.getId() && fail.getTarget() != global.getId()) {
                        Main.log.warn("FAIL target stream ID does not match the expected stream: " + fail.getTarget());
                        Main.log.warn("Check how the server reports FAIL target in your protocol.");
                        return false;
                    }

                    if (!expectedCodes.contains(fail.getCode())) {
                        Main.log.warn("Received unexpected FAIL code: " + fail.getCode());
                        Main.log.warn("Server might not be enforcing payload length contracts correctly.");
                    } else {
                        Main.log.info("FAIL code is acceptable for this test: " + fail.getCode());
                    }

                } catch (CompletionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof IOException io) {
                        Main.log.severe("An error occurred while reading FAIL from global stream: " + io.getMessage());
                    } else if (cause instanceof TimeoutException to) {
                        Main.log.severe("Timeout while waiting for FAIL on the global stream: " + to);
                    } else if (cause instanceof IllegalArgumentException fail) {
                        Main.log.severe("Invalid FAIL frame: " + fail.getMessage());
                    } else {
                        Main.log.severe("An internal error occurred while reading FAIL: " + cause);
                    }

                    return false;
                }
            }

            Main.log.info("Success");
            Main.log.info("Closing output stream");
            output.close();
            return true;

        } catch (ConnectionException e) {
            Main.log.severe("Error while establishing connection: " + e);
        } catch (PoolException e) {
            Main.log.severe("Error while creating or using a pool: " + e);
        } catch (IOException e) {
            Main.log.severe("I/O error while using the connection: " + e.getMessage());
        } catch (TimeoutException e) {
            Main.log.severe("Timeout while waiting for a global stream: " + e.getMessage());
        }

        return false;
    }
}
