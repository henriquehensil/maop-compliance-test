package dev.hensil.maop.compliance.client.situation.request;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.*;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataInput;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataOutput;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.situation.Situation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class NormalRequestWithBlocksSituation implements Situation {

    private static final long WARN_THRESHOLD_MS = 300L;

    @Override
    public @NotNull String getName() {
        return "Normal REQUEST with blocks (timing checks)";
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

            Main.log.trace("Creating a bidirectional stream for a normal REQUEST with blocks");
            stream = connection.getPools().createBidirectional();
            final int streamId = stream.getId();
            Main.log.trace("Bidirectional stream id: " + streamId);

            @NotNull UnidirectionalDataOutput output = stream;

            short msgId = 1;
            long payloadLen = 1_000L;
            byte priority = 0;
            short responseId = 1;
            int timeoutMs = 3_000;

            Main.log.info("Sending normal REQUEST with payload_len = " + payloadLen + " on bidirectional stream");

            output.writeRequest(msgId, payloadLen, priority, responseId, timeoutMs);
            Main.log.info("REQUEST written successfully on stream " + streamId);

            Main.log.info("Waiting for PROCEED on a global control stream");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(5000);

            @NotNull CompletableFuture<long[]> proceedFuture = new CompletableFuture<>();
            proceedFuture.orTimeout(5, TimeUnit.SECONDS);

            CompletableFuture.runAsync(() -> {

                try {
                    Main.log.info("Reading PROCEED from global stream " + global.getId());

                    long start = System.nanoTime();
                    long @NotNull [] allowedStreams = global.readProceed();
                    long end = System.nanoTime();

                    long elapsedMs = (end - start) / 1_000_000L;
                    if (elapsedMs > WARN_THRESHOLD_MS) {
                        Main.log.warn("PROCEED read took too long: " + elapsedMs + " ms (limit: " + WARN_THRESHOLD_MS + " ms)");
                    } else {
                        Main.log.trace("PROCEED read time: " + elapsedMs + " ms");
                    }

                    proceedFuture.complete(allowedStreams);
                } catch (@NotNull Throwable e) {
                    proceedFuture.completeExceptionally(e);
                }
            });

            long @NotNull [] allowedStreams;
            try {
                allowedStreams = proceedFuture.join();
                Main.log.info("PROCEED successfully read: " + Arrays.toString(allowedStreams));
            } catch (@NotNull CompletionException e) {
                @NotNull Throwable cause = e.getCause();
                if (cause instanceof TimeoutException to) {
                    Main.log.severe("Timeout while waiting for PROCEED: " + to);
                } else if (cause instanceof IOException io) {
                    Main.log.severe("I/O error while reading PROCEED: " + io.getMessage());
                } else if (cause instanceof IllegalArgumentException fail) {
                    Main.log.severe("Invalid PROCEED frame: " + fail.getMessage());
                } else {
                    Main.log.severe("An internal error occurred while reading PROCEED: " + cause);
                }

                return false;
            }

            boolean allowed = Arrays.stream(allowedStreams)
                    .anyMatch(id -> id == streamId);

            if (!allowed) {
                Main.log.severe("PROCEED does not authorize the REQUEST stream id " + streamId);
                Main.log.severe("Server did not accept a structurally valid REQUEST with blocks.");
                return false;
            }

            Main.log.info("PROCEED correctly authorizes this REQUEST stream: " + streamId);

            Main.log.info("Sending BLOCKs for payload_len = " + payloadLen);

            long remaining = payloadLen;
            int blockChunkSize = 256;

            while (remaining > 0) {
                int currentSize = (int) Math.min(blockChunkSize, remaining);
                byte @NotNull [] chunk = new byte[currentSize];

                output.writeBlock(chunk);
                Main.log.trace("BLOCK of size " + currentSize + " written on stream " + streamId);

                remaining -= currentSize;
            }

            Main.log.info("All BLOCKs sent, total payload written: " + payloadLen);

            Main.log.info("Sending BLOCK_END with total_length = " + payloadLen);
            output.writeBlockEnd(payloadLen);
            Main.log.info("BLOCK_END sent successfully on stream " + streamId);

            Main.log.info("Checking that no FAIL is reported for this REQUEST in a short interval");

            @NotNull CompletableFuture<Fail> failFuture = new CompletableFuture<>();
            failFuture.orTimeout(2, TimeUnit.SECONDS);

            CompletableFuture.runAsync(() -> {
                try {
                    Main.log.trace("Trying to read a FAIL from the same global stream (if any)");

                    long start = System.nanoTime();
                    @NotNull Fail fail = global.readFail();
                    long end = System.nanoTime();

                    long elapsedMs = (end - start) / 1_000_000L;
                    if (elapsedMs > WARN_THRESHOLD_MS) {
                        Main.log.warn("FAIL read took too long: " + elapsedMs + " ms (limit: " + WARN_THRESHOLD_MS + " ms)");
                    } else {
                        Main.log.trace("FAIL read time: " + elapsedMs + " ms");
                    }

                    failFuture.complete(fail);
                } catch (@NotNull Throwable e) {
                    failFuture.completeExceptionally(e);
                }
            });

            try {
                @NotNull Fail fail = failFuture.join();
                Main.log.severe("Received FAIL after a supposedly valid REQUEST with blocks: " + fail);

                long failTarget = fail.getTarget();

                if (failTarget == streamId) {
                    Main.log.severe("FAIL explicitly targets the REQUEST stream. This should not happen in a normal flow.");
                } else {
                    Main.log.severe("FAIL targets another stream (" + failTarget + ") after a valid REQUEST with blocks.");
                }

                return false;

            } catch (@NotNull CompletionException e) {
                @NotNull Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    Main.log.info("No FAIL received for this REQUEST within the check window – OK.");
                    return true;
                }

                if (cause instanceof IOException io) {
                    Main.log.severe("I/O error while checking for FAIL: " + io.getMessage());
                } else if (cause instanceof IllegalArgumentException fail) {
                    Main.log.severe("Invalid FAIL frame while checking: " + fail.getMessage());
                } else {
                    Main.log.severe("An internal error occurred while checking for FAIL: " + cause);
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