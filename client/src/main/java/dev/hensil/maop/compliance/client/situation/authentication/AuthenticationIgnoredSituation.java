package dev.hensil.maop.compliance.client.situation.authentication;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.BidirectionalStream;
import dev.hensil.maop.compliance.client.connection.Connection;
import dev.hensil.maop.compliance.client.connection.Connections;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.Fail;
import dev.hensil.maop.compliance.client.protocol.authentication.Approved;
import dev.hensil.maop.compliance.client.protocol.authentication.Disapproved;
import dev.hensil.maop.compliance.client.protocol.authentication.Result;
import dev.hensil.maop.compliance.client.situation.Situation;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class AuthenticationIgnoredSituation implements Situation {

    // Objects

    @Override
    public @NotNull String getName() {
        return "Initial authentication ignored";
    }

    @Override
    public boolean execute() {
        Main.log.info("Creating connection and connect to server...");
        try (
                @NotNull Connection connection = Connections.getInstance().create("Authentication ignored");
                @NotNull BidirectionalStream stream = connection.getPools().createBidirectional()
        ) {
            Main.log.info("Bidirectional stream created!");
            Main.log.info("Starting to write a Request operation");

            int timeout = 3;
            @NotNull TimeUnit unit = TimeUnit.SECONDS;
            {
                @NotNull CompletableFuture<Void> future = new CompletableFuture<>();
                future.orTimeout(2, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        long ms = System.currentTimeMillis();
                        stream.writeRequest((short) 1, 0L, (byte) 0, (short) 1, 1000);
                        int time = Math.toIntExact(ms - System.currentTimeMillis());

                        if (time > 300) {
                            Main.log.warn("The connection took " + time + " milliseconds to read the response slowly!");
                        } else {
                            Main.log.warn("The connection took " + time + " milliseconds to read the response");
                        }

                        future.complete(null);
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                });

                try {
                    future.join();
                    Main.log.info("Successfully written");
                } catch (CompletionException e) {
                    if (e.getCause() instanceof IOException io) {
                        Main.log.severe("Error while attempting to write on stream connection: " + io.getMessage());
                        return false;
                    } else if (e.getCause() instanceof TimeoutException) {
                        Main.log.severe("The timeout stream to write has expired: " + e.getClass().getSimpleName());
                        Main.log.severe("The timeout was set as " + timeout + " " + unit + " to write and the timeout was reached");
                        return false;
                    }

                    throw new Exception("Internal error");
                }
            }

            Main.log.trace("Starting to read the server response");

            @NotNull CompletableFuture<Result> future = new CompletableFuture<>();
            future.orTimeout(3, TimeUnit.SECONDS);

            CompletableFuture.runAsync(() -> {
                try {
                    long ms = System.currentTimeMillis();
                    byte @NotNull [] result = stream.readResultLegacy();
                    int time = Math.toIntExact(ms - System.currentTimeMillis());

                    if (time > 500 && time <= 1000) {
                        Main.log.warn("The connection took " + time + " milliseconds to read the response in a reasonable way");
                    } else if (time > 1000) {
                        Main.log.warn("The connection took " + time + " milliseconds to read the response slowly!!");
                    }

                    future.complete(Result.parse(ByteBuffer.wrap(result)));
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }
            });

            @NotNull Result result;

            try {
                result = future.join();
                Main.log.info("Successfully read");
                Main.log.info("Starting response verifications");

                if (result instanceof Approved) {
                    Main.log.severe("The response should have been rejected, but it was approved.");
                    return true;
                }

                @NotNull Disapproved disapproved = (Disapproved) result;

                @NotNull List<Short> expectedCode = new ArrayList<>() {{
                    this.add(Fail.Code.UNAUTHORIZED.getValue());
                    this.add(Fail.Code.ORDER_VIOLATION.getValue());
                    this.add(Fail.Code.ILLEGAL_STREAM.getValue());
                    this.add(Fail.Code.ORDER_VIOLATION.getValue());
                }};

                if (!expectedCode.contains(disapproved.getCode())) {
                    Main.log.warn("The error code was not as expected!");
                }

                if (!disapproved.getVersion().equals(Main.getVersion())) {
                    throw new InvalidVersionException("Version type different from what was defined: " + disapproved.getVersion());
                }

                Main.log.info("Error code: " + disapproved.getCode());
                Main.log.info("Reason: " + disapproved.getReason());
            } catch (CompletionException e) {
                if (e.getCause() instanceof IOException io) {
                    Main.log.severe("Error while attempting to read on stream connection: " + io.getMessage());
                    return true;
                } else if (e.getCause() instanceof TimeoutException) {
                    Main.log.severe("The timeout stream to read has expired: " + e.getClass().getSimpleName());
                    Main.log.severe("The timeout was set as " + timeout + " " + unit + " to read and the timeout was reached");
                    return true;
                }

                throw new Exception("Internal error");
            }
        } catch (ConnectionException e) {
            Main.log.severe("Error while attempting to establish connection: " + e.getClass().getSimpleName());
            Main.log.severe(e.toString());
        } catch (PoolException e) {
            Main.log.severe("Error while attempting to create a new pool: " + e.getClass().getSimpleName());
            Main.log.severe(e.toString());
        } catch (IOException e) {
            Main.log.severe("Error while attempting to use connection: " + e.getMessage());
        } catch (TimeoutException e) {
            Main.log.severe("The timeout stream to write or read has expired: " + e.getClass().getSimpleName());
            Main.log.severe("Took longer than 2 seconds to write or read and the timeout was reached");
        } catch (Throwable e) {
            Main.log.severe("An unknown error occurs: " + e);
        }

        return true;
    }
}