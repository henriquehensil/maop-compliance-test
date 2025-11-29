package dev.hensil.maop.compliance.client.situation.block;

import org.jetbrains.annotations.NotNull;

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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class MissingBlockEndSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "Missing BLOCK_END after MESSAGE payload";
    }

    @Override
    public boolean execute() {
        try {
            Main.log.info("Trying to obtain an authenticated connection");
            @NotNull Connection connection = Connections.getInstance().getOrCreate("normal");

            if (!connection.isAuthenticated()) {
                Main.log.severe("Connection is not authenticated. This situation requires a pre-authenticated connection.");
                return false;
            }

            Main.log.trace("Creating a unidirectional stream for MESSAGE/BLOCK sequence");
            @NotNull UnidirectionalDataOutput output = connection.getPools().createUnidirectional();

            {
                long declaredPayloadLength = 256L;
                short msgId = 1;
                byte priority = 0;

                Main.log.info("Sending MESSAGE with declared payload_len = " + declaredPayloadLength);

                int capacity = 1 + Operation.MESSAGE.getHeaderLength();
                @NotNull ByteBuffer buffer = ByteBuffer.allocate(capacity);

                buffer.put(Operation.MESSAGE.getCode())
                        .putShort(msgId)
                        .putLong(declaredPayloadLength)
                        .put(priority);

                buffer.flip();
                byte @NotNull [] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);

                output.write(bytes);
                Main.log.info("MESSAGE frame written successfully");
            }

            Main.log.info("Waiting for PROCEED frame on a global control stream");
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
                    long @NotNull [] allowed = proceedFuture.join();
                    Main.log.trace("Streams allowed by PROCEED: " + Arrays.toString(allowed));

                    boolean allowedForThisStream = Arrays.stream(allowed)
                            .anyMatch(id -> id == output.getId());

                    if (!allowedForThisStream) {
                        Main.log.severe("PROCEED did not include the payload stream id " + output.getId());
                        Main.log.severe("Server is not authorizing the stream that sent the MESSAGE.");
                        return false;
                    }
                } catch (@NotNull CompletionException e) {
                    @NotNull Throwable cause = e.getCause();
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

            Main.log.info("Sending a few BLOCK frames and closing the stream without BLOCK_END");

            int blockCount = 3;
            int blockSize = 64;
            long totalSent = 0L;

            for (int i = 0; i < blockCount; i++) {
                byte @NotNull [] payload = new byte[blockSize];

                Main.log.trace("Sending BLOCK " + (i + 1) + "/" + blockCount + " with size " + blockSize);

                @NotNull ByteBuffer buffer = ByteBuffer.allocate(1 + Operation.BLOCK.getHeaderLength() + payload.length);
                buffer.put(Operation.BLOCK.getCode())
                        .putInt(payload.length)
                        .put(payload);

                buffer.flip();

                output.write(buffer.array(), buffer.position(), buffer.limit());
                totalSent += payload.length;
            }

            Main.log.info("Total payload bytes sent via BLOCKs: " + totalSent);
            Main.log.info("Closing the output stream without sending BLOCK_END");

            output.close();

            Main.log.info("Waiting for FAIL frame on a global control stream due to missing BLOCK_END");
            {
                @NotNull CompletableFuture<Fail> future = new CompletableFuture<>();
                future.orTimeout(5, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.info("Reading FAIL from global stream " + global.getId());
                        @NotNull Fail fail = global.readFail();
                        future.complete(fail);
                    } catch (@NotNull Throwable e) {
                        future.completeExceptionally(e);
                    }
                });

                try {
                    @NotNull Fail fail = future.join();

                    Main.log.info("FAIL frame successfully read");
                    Main.log.info("Starting validations");

                    @NotNull Set<Fail.Code> expectedCodes = new HashSet<>() {{
                        add(Fail.Code.TIMEOUT);
                        add(Fail.Code.CANCELLED);
                        add(Fail.Code.ORDER_VIOLATION);
                        add(Fail.Code.PROTOCOL_VIOLATION);
                        add(Fail.Code.INVALID_FORMAT);
                    }};

                    Main.log.trace("Accepted FAIL codes for this situation: " + expectedCodes);

                    if (!expectedCodes.contains(fail.getCode())) {
                        Main.log.warn("Received unexpected FAIL code: " + fail.getCode());
                        Main.log.warn("Server might not be enforcing missing BLOCK_END contracts as expected.");
                    }

                    Main.log.info("FAIL code is acceptable for this test: " + fail.getCode());
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

            return true;
        } catch (@NotNull ConnectionException e) {
            Main.log.severe("Error while establishing connection: " + e);
        } catch (@NotNull PoolException e) {
            Main.log.severe("Error while creating or using a pool: " + e);
        } catch (@NotNull IOException e) {
            Main.log.severe("I/O error while using the connection: " + e.getMessage());
        } catch (@NotNull TimeoutException e) {
            Main.log.severe("Timeout while waiting for a global stream: " + e.getMessage());
        }

        return false;
    }
}

