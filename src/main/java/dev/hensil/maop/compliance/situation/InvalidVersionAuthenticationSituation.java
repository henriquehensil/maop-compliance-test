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
import dev.hensil.maop.compliance.model.*;
import dev.hensil.maop.compliance.model.authentication.Approved;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/*
* Servidores devem estar ciente que haverá múltiplas tentativas de envios inválidos
* Portanto, é ideal permitirem o maior número de conexões possíveis aceitas para este teste.
* Servidores cujo sistema de segurança bloqueia novas conexões causada por este teste podem afetar diagnósticos seguintes.
*
* Caso haja Disapproved como resposta, então, espera-se "INVALID_FORMAT" e "PROTOCOL_VIOLATION" em Disapproved#ERROR_CODE
* */
@Plugin
@Category("Situation")
final class InvalidVersionAuthenticationSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(InvalidVersionAuthenticationSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Authentication")
        ) {
            log.info("Starting authentication with invalid version diagnostic");
            log.info("Loading authentication presets");
            @NotNull Authentication authentication = new Authentication(compliance.getPreset());

            @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                this.add(MAOPError.INVALID_FORMAT);
                this.add(MAOPError.PROTOCOL_VIOLATION);
            }};

            @NotNull String @NotNull [] invalidVersions = new String[] {
                    "1.2.3.4", "v1.2.3", "01.2.3", "1.02.3", "1.2.03", "00.1.1", "1.2.3-",
                    "1.2.3-alpha.-beta", "1.2.3-alpha.", "1.2.3-.", "1.2.3-.-", "1.2.3-alpha..",
                    "1.2.3+build..", "1.2.3-alpha.", "1.2.3-.alpha", "1.2.3-alpha_beta", "1.2.3-alpha 1",
                    "1.2.3-alpha.01", "1.2.3-00", "1.2.3-01",
            };

            @Nullable BidirectionalStream stream = null;
            @Nullable Connection connection = null;
            @Nullable Duration retryAfter = null;

            for (int i = 0; i < invalidVersions.length; i++) {
                log.info("Attempt " + i + " for writing authentication with invalid version");

                // Replace the correct version for invalid
                @NotNull String invalidVersion = invalidVersions[i];
                @NotNull Authentication invalidAuth = new Authentication(
                        authentication.getType(),
                        authentication.getToken(),
                        authentication.getMetadata(),
                        invalidVersion,
                        authentication.getVendor());

                @NotNull ByteBuffer buffer = invalidAuth.toByteBuffer();

                try (
                        @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                .put("authentication type", authentication.getType())
                                .put("authentication metadata", authentication.getMetadata())
                                .put("authentication vendor", authentication.getVendor())
                                .put("authentication invalid version", invalidVersion)
                                .put("authentication total length", buffer.limit())
                                .install()
                ) {
                    // Creating connection
                    if (connection == null) {
                        connection = compliance.createConnection("authenticationInvalidVersion", this);
                        stream = connection.createBidirectionalStream();
                    }

                    if (retryAfter != null) {
                        log.info("Waiting the \"retry_after\" be exceeded (" + retryAfter +")");
                        LockSupport.parkNanos(retryAfter.toNanos() + 1);
                    }

                    try (
                            @NotNull LogCtx.Scope logContext3 = LogCtx.builder()
                                    .put("connection", connection.toString())
                                    .put("retry after ms", retryAfter == null ? 0 : retryAfter.toMillis())
                                    .put("stream id", stream.getId())
                                    .put("bidirectional", true)
                                    .install();

                            @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
                    ) {
                        log.info("Writing authentication with invalid version: " + invalidVersions[i]);
                        stream.write(buffer.array(), 0, buffer.limit());
                        log.info("Authentication sent successfully");

                        log.info("Waiting for some server behaviour");
                        boolean closed = connection.awaitDisconnection(2, TimeUnit.SECONDS);
                        if (closed) {
                            log.info("Connection was closed immediately and correctly");
                            connection = null;
                            continue;
                        }

                        log.info("Waiting for authentication response");
                        @NotNull Result result;
                        try {
                            @NotNull Elapsed elapsed = new Elapsed();
                            result = Result.readResult(stream, 5, TimeUnit.SECONDS);
                            elapsed.freeze();

                            try (
                                    @NotNull LogCtx.Scope logContext4 = LogCtx.builder()
                                            .put("approved", result.isApproved())
                                            .put("result version", result.getVersion())
                                            .put("result vendor", result.getVendor())
                                            .put("result length", result.getLength())
                                            .put("elapsed", elapsed)
                                            .install();

                                    @NotNull Stack.Scope logScope3 = Stack.pushScope("Verification")
                            ) {
                                log.info("The server takes " + elapsed + " milliseconds to build a Result");

                                if (result instanceof Approved) {
                                    log.severe("Authentication with invalid version (" + invalidVersion + ") was accepted with APPROVED");
                                    return true;
                                }

                                @NotNull Disapproved disapproved = (Disapproved) result;
                                @Nullable MAOPError error = MAOPError.get(disapproved.getErrorCode());
                                if (error == null) {
                                    log.severe("Maop error code not found: " + disapproved.getErrorCode());
                                    return true;
                                }

                                if (!expectedErrors.contains(error)) {
                                    log.warn("Error code \"" + error + "\" not suitable for the situation");
                                }

                                log.info("Successfully disapproved with reason: " + disapproved.getReason());
                                @Nullable Duration duration = disapproved.getRetryAfter();
                                boolean canRetry = duration != null && duration.compareTo(Duration.ofSeconds(3)) < 0;
                                if (!canRetry) {
                                    log.warn("The \"retry after\" disapproved field is too long (" + duration + ") and doest will not be reused");

                                    try {
                                        connection.close();
                                    } catch (IOException e) {
                                        log.warn("Cannot close connection: " + e);
                                    }

                                    connection = null;
                                    stream = null;

                                    continue;
                                }

                                log.info("Reusing connection");
                                retryAfter = duration;
                            }
                        } catch (Throwable e) {
                            if (connection != null && connection.isClosed()) {
                                log.info("Connection was closed immediately and correctly after reading the Result");
                                connection = null;
                                continue;
                            }

                            log.severe("Read Result failed: " + e);
                            return true;
                        }
                    }
                } catch (DirectionalStreamException e) {
                    log.severe("Failed to create bidirectional authentication stream: " + e);

                    if (!connection.isConnected()) {
                        log.warn("Connection was closed before authentication could start");
                    }

                    return true;
                } catch (ConnectionException e) {
                    if (i < 2) {
                        log.severe("Failed to create authentication connection: " + e.getCause());
                        return true;
                    }

                    log.warn("It was only possible to test " + i + " possibilities of authentication with invalid version");
                } catch (IOException e) {
                    log.severe("Write failed: " + e);

                    if (!connection.isConnected()) {
                        log.severe("Connection closed while sending authentication request: " + e);
                    }

                    return true;
                }
            }
        }

        return false;
    }
}