package dev.hensil.maop.compliance.client.situation.message;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.*;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataInput;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.Operation;
import dev.hensil.maop.compliance.client.situation.Situation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class MessageOnBidirectionalStreamSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "MESSAGE sent on bidirectional stream";
    }

    @Override
    public boolean execute() {
        @Nullable BidirectionalStream stream = null;

        try {
            Main.log.info("Trying to obtain an authenticated connection");
            @NotNull Connection connection = Connections.getInstance().getOrCreate("normal");

            if (!connection.isAuthenticated()) {
                Main.log.severe("Connection is not authenticated. This situation requires a pre-authenticated connection.");
                return false;
            }

            Main.log.trace("Creating a bidirectional stream to send an illegal MESSAGE");
            stream = connection.getPools().createBidirectional();
            final int streamId = stream.getId();
            Main.log.trace("Bidirectional stream id: " + streamId);

            Main.log.info("Sending MESSAGE (msg_id = 1) on a bidirectional stream (expected to be illegal)");

            {
                int capacity = 1 + Operation.MESSAGE.getHeaderLength();
                @NotNull ByteBuffer buffer = ByteBuffer.allocate(capacity);

                buffer.put(Operation.MESSAGE.getCode())
                        .putShort((short) 1)
                        .putLong(100L)
                        .put((byte) 0);

                buffer.flip();

                stream.write(buffer.array(), buffer.position(), buffer.limit());
                Main.log.info("Illegal MESSAGE frame written on bidirectional stream " + streamId);
            }

            Main.log.info("Waiting for a global control stream to read FAIL");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(5000);

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

                if (failTarget != streamId) {
                    Main.log.severe(
                            "FAIL target stream ID does not match the bidirectional stream used. " +
                                    "Expected: " + streamId + ", received: " + failTarget
                    );
                    Main.log.severe("Server is reporting FAIL for the wrong stream. This is not acceptable.");
                    return false;
                }

                @NotNull Set<Fail.Code> expectedCodes = new HashSet<>() {{
                    add(Fail.Code.ILLEGAL_STREAM);
                    add(Fail.Code.PROTOCOL_VIOLATION);
                    add(Fail.Code.INVALID_FORMAT);
                    add(Fail.Code.ORDER_VIOLATION);
                }};

                Main.log.trace("Accepted FAIL codes for this situation: " + expectedCodes);

                if (!expectedCodes.contains(fail.getCode())) {
                    Main.log.warn("Received unexpected FAIL code: " + fail.getCode());
                    Main.log.warn("Server is reacting, but with a non-standard error code for MESSAGE on bidirectional stream.");
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
            if (stream != null) {
                try {
                    Main.log.info("Closing bidirectional stream");
                    stream.close();
                } catch (@NotNull IOException e) {
                    Main.log.warn("Error while closing bidirectional stream: " + e.getMessage());
                }
            }
        }
    }
}