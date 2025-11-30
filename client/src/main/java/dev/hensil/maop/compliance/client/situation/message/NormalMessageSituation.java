package dev.hensil.maop.compliance.client.situation.message;

import dev.hensil.maop.compliance.client.Main;
import dev.hensil.maop.compliance.client.connection.*;
import dev.hensil.maop.compliance.client.connection.BidirectionalStream;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataInput;
import dev.hensil.maop.compliance.client.connection.UnidirectionalDataOutput;
import dev.hensil.maop.compliance.client.exception.ConnectionException;
import dev.hensil.maop.compliance.client.exception.InvalidVersionException;
import dev.hensil.maop.compliance.client.exception.PoolException;
import dev.hensil.maop.compliance.client.protocol.authentication.Disapproved;
import dev.hensil.maop.compliance.client.protocol.authentication.Result;
import dev.hensil.maop.compliance.client.situation.Situation;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class NormalMessageSituation implements Situation {

    @Override
    public @NotNull String getName() {
        return "Normal message";
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
            Main.log.info("Writing message operation with " + payload + " payload on stream with id " + output.getId());
            output.writeMessage((short) 1, payload, (byte) 0);
            Main.log.info("Successfully written");

            Main.log.trace("Waiting the creation of a global stream");
            @NotNull UnidirectionalDataInput global = connection.getPools().waitGlobalStream(1000);
            Main.log.trace("Global stream created");

            Main.log.info("Reading confirmation");

            @NotNull CompletableFuture<long[]> future = new CompletableFuture<>();
            {
                Main.log.trace("Prepare timeout to read");
                future.orTimeout(2, TimeUnit.SECONDS);

                CompletableFuture.runAsync(() -> {
                    try {
                        Main.log.debug("Starting read proceeds");
                        long @NotNull [] proceeds = global.readProceed();
                        Main.log.debug("Successfully read");
                        future.complete(proceeds);
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                    }
                });
            }

            try {
                long @NotNull [] proceeds = future.join();
                if (proceeds.length > 1) {
                    Main.log.severe("This connection sent only 1 request but received a " + proceeds.length + "\"Proceed\" operations.");
                    return false;
                }

                if (proceeds.length == 0) {
                    Main.log.severe("This connection receive a proceed with invalid \"count\": " + proceeds.length);
                    return false;
                }

                if (proceeds[0] != output.getId()) {
                    Main.log.severe("This connection received a \"Proceed\" but with a different stream ID than it should be: " + proceeds[0]);
                    return false;
                }

                Main.log.info("Successfully read \"Proceed\" operation");
            } catch (CompletionException e) {
                if (e.getCause() instanceof IOException io) {
                    Main.log.severe("An error occurred while trying to read \"Proceed\": " + io.getMessage());
                } else {
                    Main.log.severe("An internal error occurs while to trying get the proceed: " + e.getCause());
                }

                return false;
            }

            Main.log.info("Writing block operation");
            Main.log.trace("Prepare to write block and configure timeout");
            {
                @NotNull CompletableFuture<Void> future1 = new CompletableFuture<>();
                future1.orTimeout(2, TimeUnit.SECONDS);
                CompletableFuture.runAsync(() -> {
                    try {
                        output.writeBlock(new byte[payload]);
                        Main.log.trace("Write block end");
                        output.writeBlockEnd(payload);
                        future.complete(null);
                    } catch (IOException e) {
                        future1.completeExceptionally(e);
                    }
                });

                try {
                    future1.join();
                    Main.log.info("Successfully written");
                } catch (CompletionException e) {
                    if (e.getCause() instanceof IOException io) {
                        Main.log.severe("An error occurs while to trying write a block operation: " + io.getMessage());
                    } else if (e.getCause() instanceof TimeoutException to) {
                        Main.log.severe("The timeout to write a block operation was expired: " + to);
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