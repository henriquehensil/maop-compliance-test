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
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.authentication.Approved;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class InvalidAuthenticationSituation extends Situation {

    public static @NotNull Logger log = Logger.create(InvalidAuthenticationSituation.class).formatter(Main.FORMATTER);

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
            connection = compliance.createConnection("invalid authentication", this);
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
                byte @NotNull [] invalidData = authentication.toByteBuffer().array();
                log.info("Filling authentication data with invalid fields");
                Arrays.fill(invalidData, (byte) 0xAB);

                try (
                        @NotNull LogCtx.Scope logContext3 = LogCtx.builder()
                                .put("authentication data", Arrays.toString(invalidData))
                                .install();

                        @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
                ) {
                    log.info("Writing authentication");
                    stream.write(invalidData);
                } catch (IOException e) {
                    if (connection.isConnected()) {
                        log.warn("Write failed, retrying once");

                        try {
                            stream.write(invalidData);
                        } catch (IOException ex) {
                            log.severe("Authentication write failed after retry: " + ex);
                               return true;
                        }
                    } else {
                        log.warn("Connection closed while sending authentication request: " + e);
                        return false;
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

                    if (result instanceof Approved) {
                        log.severe("Server approved invalid authentication");
                        return true;
                    }

                    @NotNull Disapproved disapproved = (Disapproved) result;
                    log.info("Received Disapproved");

                    @Nullable MAOPError error = MAOPError.get(disapproved.getErrorCode());
                    if (error == null) {
                        log.severe("Unknown error code was received in disapproved: " + disapproved.getErrorCode());
                        return true;
                    }

                    log.info("Successfully Disapproved. Error: " + error + " | Reason: " + disapproved.getReason());

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
            if (!connection.isClosed()) {
                log.severe("Result read timed out: " + e.getMessage());
                log.warn("Connection still alive");
                return true;
            }

            log.info("The servers close connection correctly");
            return false;
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create bidirectional stream: " + e.getMessage());

            if (!connection.isConnected()) {
                log.warn("Connection was closed before authentication could start");
            }

            return true;
        } catch (IOException e) {
            log.severe("A error occurs while read result from the server: " + e.getMessage());
            return true;
        }
    }
}
