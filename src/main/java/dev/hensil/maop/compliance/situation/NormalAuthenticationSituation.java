package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.Elapsed;
import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.Main;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Approved;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;
import dev.meinicke.plugin.annotation.Priority;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Priority(value = 1)
final class NormalAuthenticationSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalAuthenticationSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        @Nullable Connection connection = null;
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Authentication")
        ) {
            log.info("Creating connection");
            connection = compliance.createConnection("authentication", this);

            log.info("Creating bidirectional stream");
            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection.toString())
                            .put("stream id", stream.getId())
                            .put("is bidirectional", true)
                            .install();
            ) {

                log.info("Loading authentication presets");
                @NotNull Authentication authentication = new Authentication(compliance.getPreset());
                byte @NotNull [] data = authentication.toByteBuffer().array();

                try (
                        @NotNull LogCtx.Scope logContext3 = LogCtx.builder()
                                .put("authentication type", authentication.getType())
                                .put("authentication metadata", authentication.getMetadata())
                                .put("authentication version", authentication.getVersion())
                                .put("authentication vendor", authentication.getVendor())
                                .put("authentication total length", data.length)
                                .install();

                        @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
                ) {
                    log.info("Writing authentication");
                    stream.write(data);
                } catch (IOException e) {
                    if (connection.isConnected()) {
                        log.warn("Write failed, retrying once");

                        try {
                            stream.write(data);
                        } catch (IOException ex) {
                            log.severe("Authentication write failed after retry: " + ex);
                            return true;
                        }
                    } else {
                        log.severe("Connection closed while sending authentication request: " + e);
                        return true;
                    }
                }

                try {
                    stream.closeOutput();
                } catch (IOException e) {
                    log.warn("Failed to close authentication output stream: " + e);

                    if (!connection.isConnected()) {
                        log.severe("Connection closed unexpectedly after authentication request");
                        return true;
                    }
                }

                log.info("Waiting for Result");

                @NotNull Elapsed elapsed = new Elapsed();
                connection.awaitReading(Result.MIN_LENGTH, stream, 3, TimeUnit.SECONDS);
                @NotNull Result result = Result.readResult(stream, 2, TimeUnit.SECONDS);
                elapsed.freeze();

                try (
                        @NotNull LogCtx.Scope logContext4 = LogCtx.builder()
                                .put("result approved", result.isApproved())
                                .put("result vendor", result.getVendor())
                                .put("result version", result.getVersion())
                                .put("elapsed", elapsed)
                                .install();

                        @NotNull Stack.Scope logScope3 = Stack.pushScope("Read")
                ) {
                    log.info("The server took " + elapsed + " to send response");

                    if (result instanceof Disapproved disapproved) {
                        log.severe("Authentication rejected with code \"" + disapproved.getErrorCode() + "\" reason \"" + disapproved.getReason() + "\"");
                        return true;
                    }

                    log.info("Successfully authenticated");
                    connection.setAuthenticated((Approved) result);

                    // Shutdown
                    try {
                        stream.closeInput();
                    } catch (IOException e) {
                        log.warn("Failed to close authentication input stream: " + e);
                        if (!connection.isConnected()) {
                            log.severe("Connection closed unexpectedly after authentication");
                            return true;
                        }
                    }

                    // Finish
                    return false;
                }
            }
        } catch (TimeoutException e) {
            log.severe("Result read timed out: " + e.getMessage());
            return true;
        } catch (ConnectionException e) {
            log.severe("Failed to create authentication connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create bidirectional authentication stream: " + e.getMessage());

            if (!connection.isConnected()) {
                log.warn("Connection was closed before authentication could start");
            }

            return true;
        } catch (IOException e) {
            log.severe("A error occurs while read result response from the server: " + e.getMessage());
            return true;
        }
    }
}