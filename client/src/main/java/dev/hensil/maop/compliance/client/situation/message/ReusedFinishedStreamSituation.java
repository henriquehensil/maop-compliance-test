package dev.hensil.maop.compliance.client.situation.message;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.*;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataInput;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataOutput;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.Operation;
import dev.hensil.maop.compliance.client.situation.Situation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ReusedFinishedStreamSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "Reusing a finished stream for new operations";
    }

    @Override
    public boolean execute() {
        @Nullable UnidirectionalDataOutput output = null;

        try {
            Main.log.info("Trying to obtain an authenticated connection");
            @NotNull Connection connection = Connections.getInstance().getOrCreate("normal");

            if (!connection.isAuthenticated()) {
                Main.log.severe("Connection is not authenticated. This situation requires a pre-authenticated connection.");
                return false;
            }

            Main.log.trace("Creating a unidirectional stream for first MESSAGE");
            output = connection.getPools().createUnidirectional();
            final int outputStreamId = output.getId();
            Main.log.trace("Payload stream id: " + outputStreamId);

            {
                Main.log.info("Sending first MESSAGE (msg_id = 1, payload_len = 0) to finalize this stream's operation");

                int capacity = 1 + Operation.MESSAGE.getHeaderLength();
                @NotNull ByteBuffer buffer = ByteBuffer.allocate(capacity);

                buffer.put(Operation.MESSAGE.getCode())
                        .putShort((short) 1)
                        .putLong(0)
                        .put((byte) 0);

                buffer.flip();

                output.write(buffer.array(), buffer.position(), buffer.limit());
                Main.log.info("First MESSAGE frame written successfully on stream " + outputStreamId);
            }

            Main.log.info("Waiting for a global control stream");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(2000);

            {
                @NotNull CompletableFuture<long[]> proceedFuture = new CompletableFuture<>();
                proceedFuture.orTimeout(2, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.trace("Reading PROCEED from global stream " + global.getId());
                        long @NotNull [] allowedStreams = global.readProceed();
                        Main.log.info("Successfully read PROCEED");
                        proceedFuture.complete(allowedStreams);
                    } catch (@NotNull Throwable e) {
                        proceedFuture.completeExceptionally(e);
                    }
                });

                try {
                    long @NotNull [] allowedStreams = proceedFuture.join();
                    Main.log.trace("Streams allowed by PROCEED: " + Arrays.toString(allowedStreams));

                    boolean containsStream = Arrays.stream(allowedStreams)
                            .anyMatch(id -> id == outputStreamId);

                    if (!containsStream) {
                        Main.log.info("Payload stream id " + outputStreamId + " is not listed in PROCEED. " +
                                "It may already be considered finished, which is consistent with payload_len = 0.");
                        return false;
                    } else {
                        Main.log.info("PROCEED still allows stream id " + outputStreamId + " (behavior depends on spec).");
                    }

                } catch (@NotNull CompletionException e) {
                    @NotNull Throwable cause = e.getCause();
                    if (cause instanceof TimeoutException to) {
                        Main.log.warn("Timeout while waiting for PROCEED on global stream: " + to);
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

            Main.log.info("Reusing the same finished stream to send a new operation (expected to be illegal)");

            {
                long payload = 100L;

                @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.MESSAGE.getHeaderLength());
                buffer.put(Operation.MESSAGE.getCode())
                        .putShort((short) 1)
                        .putLong(payload)
                        .put((byte) 0);

                buffer.flip();

                output.write(buffer.array(), buffer.position(), buffer.limit());
                Main.log.info("Second MESSAGE frame written on finished stream " + outputStreamId);
            }

            Main.log.info("Waiting for FAIL frame");

            {
                @NotNull CompletableFuture<Fail> failFuture = new CompletableFuture<>();
                failFuture.orTimeout(5, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.info("Reading FAIL from global stream " + global.getId());
                        @NotNull Fail fail = global.readFail();
                        failFuture.complete(fail);
                    } catch (@NotNull Throwable e) {
                        failFuture.completeExceptionally(e);
                    }
                });

                try {
                    @NotNull Fail fail = failFuture.join();

                    Main.log.info("FAIL frame successfully read");
                    Main.log.info("Starting validations");

                    long failTarget = fail.getTarget();
                    Main.log.trace("FAIL target stream id: " + failTarget);

                    if (failTarget != outputStreamId) {
                        Main.log.severe("FAIL target stream ID does not match the reused stream id " + outputStreamId +
                                ". Received target: " + failTarget);
                        Main.log.severe("Server is reporting FAIL for the wrong stream. This is not acceptable.");
                        return false;
                    }

                    @NotNull Set<Fail.Code> expectedCodes = new HashSet<>() {{
                        add(Fail.Code.PROTOCOL_VIOLATION);
                        add(Fail.Code.ILLEGAL_STREAM);
                        add(Fail.Code.ORDER_VIOLATION);
                        add(Fail.Code.INVALID_FORMAT);
                    }};

                    Main.log.trace("Accepted FAIL codes for this situation: " + expectedCodes);

                    if (!expectedCodes.contains(fail.getCode())) {
                        Main.log.warn("Received unexpected FAIL code: " + fail.getCode());
                        Main.log.warn("Server is reacting, but with a non-standard error code. Check the protocol contracts.");
                        return true;
                    }

                    Main.log.info("FAIL code is acceptable for this test: " + fail.getCode());
                    return true;

                } catch (@NotNull CompletionException e) {
                    @NotNull Throwable cause = e.getCause();
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

        } catch (@NotNull ConnectionException e) {
            Main.log.severe("Error while establishing connection: " + e);
            return false;
        } catch (@NotNull PoolException e) {
            Main.log.severe("Error while creating or using a pool: " + e);
            return false;
        } catch (@NotNull IOException e) {
            Main.log.severe("I/O error while using the connection: " + e.getMessage());
            return false;
        } catch (@NotNull TimeoutException e) {
            Main.log.severe("Timeout while waiting for a global stream: " + e.getMessage());
            return false;
        } finally {
            if (output != null) {
                try {
                    Main.log.info("Closing output stream");
                    output.close();
                } catch (@NotNull IOException e) {
                    Main.log.warn("Error while closing output stream: " + e.getMessage());
                }
            }
        }
    }
}

