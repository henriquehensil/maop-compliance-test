package dev.hensil.maop.compliance.situation.authentication;

import com.jlogm.Logger;
import com.jlogm.utils.Coloured;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Approved;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.model.operation.*;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

@Plugin
@Category("Situation")
@Dependency(type = PreAuthenticationIllegalStreamSituation.class)
final class UnauthenticatedSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(UnauthenticatedSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting diagnostic");

        // Operations
        @NotNull Operation @NotNull [] operations = new Operation[] {
                new Request((short) 1, (short) 0, 0L, (byte) 0, 1000),
                new Message((short) 1, 0L, (byte) 0),
                new Block(new byte[100]),
                new BlockEnd(100),
                new DisconnectRequest(500, "Testing"),
                new Disconnect(),
                new Done(new Done.Entry[] {new Done.Entry(1L, System.currentTimeMillis(), 500)}),
                new Fail(1L, (short) 49, "Testing"),
                new Proceed(new Proceed.Entry[]{new Proceed.Entry(1L)}),
                new Refuse(new Refuse.Entry[]{new Refuse.Entry(1L, 200, (short) 49)}),
                new Response(0L, System.currentTimeMillis(), 200)
        };

        log.info("Preparing operations events");

        for (int i = 0; i < operations.length; i++) {
            @NotNull Operation operation = operations[i];

            try {
                @NotNull Connection connection = getCompliance().createConnection("Unauthenticated#" + i, this);
                @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

                log.debug("# " + i + ": Writing " + operation);

                try {
                    stream.write(operation.toBytes());
                } catch (IOException e) {
                    if (connection.isConnected()) {
                        log.warn("Write failed, retrying once");

                        try {
                            stream.write(operation.toBytes());
                        } catch (IOException ex) {
                            log.severe("Authentication write failed after retry: " + ex);
                            return true;
                        }
                    } else {
                        log.severe("Connection closed while sending authentication request: " + e);
                        return true;
                    }
                }

                log.info("Successfully written \"" + operation + "\"");

                log.info("Waiting server response");

                LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
                if (!connection.isConnected()) {
                    log.warn("The connection has been closed.");
                }

                // Try to read some response if it has

                @NotNull ByteBuffer buffer = ByteBuffer
                        .allocate(1 +
                                (16 + 1 + Byte.MAX_VALUE) +
                                (2 + 4 + 2 + Short.MAX_VALUE) +
                                1 + Byte.MAX_VALUE + 1 + Byte.MAX_VALUE
                        );

                @NotNull CompletableFuture<Boolean> future = new CompletableFuture<>();
                future.orTimeout(3, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        // Read data
                        int minDataApproved = 1 + (16 + 1 + 1) + 1 + 1 + 1 + 1;
                        int minDataDisapproved = 1 + (2 + 4 + 2 + 1) + 1 + 1 + 1 + 1;

                        long currentMs = System.currentTimeMillis();
                        // Reading
                        int read = stream.read(buffer.array());
                        int totalTime = Math.toIntExact(System.currentTimeMillis() - currentMs);

                        boolean isApproved = buffer.get(0) != 0;
                        int expected = isApproved ? minDataApproved : minDataDisapproved;

                        // If insufficient data
                        if (read < expected) {
                            log.severe("Incomplete authentication response: read " + read + " bytes, expected at least " + expected);

                            if (stream.available() > 0) {
                                log.warn("It appears the data is arriving late. Please check the stream writing implementation to fix inconsistency.");
                            }

                            future.complete(true);
                            return;
                        }

                        // Show performance
                        if (totalTime > 500) {
                            log.warn("Authentication response read " + Coloured.of("slowly").color(Color.RED).print() + ":" + read + " bytes in " + totalTime + " ms");
                        } else if (totalTime > 100) {
                            log.warn("Authentication response read reasonable: " + read + " bytes in " + totalTime + " ms");
                        } else {
                            log.info("Authentication response successfully receive: " + read + " bytes in " + totalTime + " ms");
                        }

                        buffer.position(0);
                        buffer.limit(read);

                        future.complete(false);
                    } catch (IOException e) {
                        log.severe("A error occurs while read result response from the server: " + e.getMessage());
                        future.complete(true);
                    }
                });

                // Retrieve
                try {
                    boolean severe = future.join();

                    if (severe) {
                        return true;
                    }

                } catch (CompletionException e) {
                    if (e.getCause() instanceof TimeoutException) {
                        log.severe("Authentication response timed out");
                        return true;
                    }

                    log.severe("Internal unknown error");

                    return true;
                }

                // Parse result if it has
                if (buffer.limit() != buffer.capacity()) {
                    log.info("Parsing authentication response");

                    @NotNull Result result = Result.parse(buffer);
                    if (result instanceof Approved) {
                        log.severe("Invalid authentication was approved.");
                        return true;
                    }

                    log.info("Checking error codes");

                    @NotNull Set<MAOPError> validErrors = new HashSet<>() {{
                        this.add(MAOPError.INVALID_FORMAT);
                        this.add(MAOPError.ORDER_VIOLATION);
                        this.add(MAOPError.ILLEGAL_STREAM);
                        this.add(MAOPError.PROTOCOL_VIOLATION);
                        this.add(MAOPError.UNKNOWN_ERROR);
                    }};

                    @NotNull Disapproved disapproved = (Disapproved) result;
                    @Nullable MAOPError error = MAOPError.get(disapproved.getErrorCode());
                    if (error == null) {
                        log.severe("Error code not found for " + disapproved.getErrorCode());
                        return true;
                    }

                    if (!validErrors.contains(error)) {
                        log.warn("Error code not suitable for the situation");
                        log.info("Error codes that may be suitable: " + validErrors);
                    }
                }
            } catch (DirectionalStreamException e) {
                log.severe("Failed to create bidirectional stream: " + e);
                return true;
            } catch (IOException e) {
                if (i == 1) {
                    log.severe("Failed to create connection: " + e.getMessage());
                    return true;
                }

                log.trace("Failed to create connection: " + e.getMessage());
                return false;
            }
        }

        // Finish
        return false;
    }
}