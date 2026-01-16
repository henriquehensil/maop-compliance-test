package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.Main;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private final @NotNull Logger log = Logger.create(InvalidVersionAuthenticationSituation.class).formatter(Main.FORMATTER);

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Invalid Version Test")
        ) {
            log.info("Starting authentication fuzzing with invalid versions");

            @NotNull Authentication preset = new Authentication(compliance.getPreset());
            @NotNull Set<MAOPError> expectedErrors = Set.of(MAOPError.INVALID_FORMAT, MAOPError.PROTOCOL_VIOLATION);

            @NotNull String @NotNull [] invalidVersions = {
                    "1.2.3.4", "v1.2.3", "01.2.3", "1.02.3", "1.2.03", "00.1.1", "1.2.3-",
                    "1.2.3-alpha.-beta", "1.2.3-alpha.", "1.2.3-.", "1.2.3-.-", "1.2.3-alpha..",
                    "1.2.3+build..", "1.2.3-alpha.", "1.2.3-.alpha", "1.2.3-alpha_beta", "1.2.3-alpha 1",
                    "1.2.3-alpha.01", "1.2.3-00", "1.2.3-01"
            };

            Connection connection = null;
            BidirectionalStream stream = null;
            Duration retryAfter = null;

            for (int i = 0; i < invalidVersions.length; i++) {
                @NotNull String currentVersion = invalidVersions[i];

                try (@NotNull Stack.Scope logScope2 = Stack.pushScope("Attempt-" + i)) {
                    if (connection == null || !connection.isConnected()) {
                        connection = compliance.createConnection("auth-fuzzing-" + i, this);
                        stream = connection.createBidirectionalStream();
                    }

                    if (retryAfter != null) {
                        log.info("Respecting server delay: " + retryAfter.toMillis() + "ms");
                        LockSupport.parkNanos(retryAfter.toNanos());
                        retryAfter = null;
                    }

                    @NotNull Authentication invalidAuth = new Authentication(
                            preset.getType(), preset.getToken(), preset.getMetadata(),
                            currentVersion, preset.getVendor()
                    );
                    @NotNull ByteBuffer buffer = invalidAuth.toByteBuffer();

                    try (
                            @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                                    .put("authentication type", invalidAuth.getType())
                                    .put("authentication metadata", invalidAuth.getMetadata())
                                    .put("authentication vendor", invalidAuth.getVendor())
                                    .put("authentication total length", buffer.limit())
                                    .put("invalid version", currentVersion)
                                    .put("stream id", stream.getId())
                                    .put("connection", connection)
                                    .install()
                    ) {
                        log.info("Sending version: " + currentVersion);
                        stream.write(buffer.array(), 0, buffer.limit());

                        if (connection.awaitDisconnection(1500, TimeUnit.MILLISECONDS)) {
                            log.info("Server correctly disconnected for version: " + currentVersion);
                            connection = null;
                            stream = null;
                            continue;
                        }

                        @NotNull Result result = Result.readResult(stream, 3, TimeUnit.SECONDS);
                        if (result instanceof Approved) {
                            log.severe("Server approved invalid version: " + currentVersion);
                            return true;
                        }

                        @NotNull Disapproved disapproved = (Disapproved) result;
                        @Nullable MAOPError error = MAOPError.get(disapproved.getErrorCode());
                        if (error == null) {
                            log.severe("Unknown error code received: " + disapproved.getErrorCode());
                            return true;
                        }

                        log.info("Received Disapproved. Error: " + error + " | Reason: " + disapproved.getReason());

                        if (!expectedErrors.contains(error)) {
                            log.warn("Unexpected error code for fuzzing: " + error);
                        }

                        @Nullable Duration serverDelay = disapproved.getRetryAfter();
                        if (serverDelay != null && serverDelay.getSeconds() < 3) {
                            log.info("Server requested retry-after: " + serverDelay.toMillis() + "ms. Connection will be reused.");
                            retryAfter = serverDelay;
                        } else {
                            if (serverDelay != null) {
                                log.warn("Retry-after too long (" + serverDelay.getSeconds() + "s). Resetting connection.");
                            }

                            try {
                                connection.close();
                            } catch (IOException ignore) {

                            }

                            connection = null;
                            stream = null;
                        }
                    }
                } catch (IOException | TimeoutException e) {
                    log.warn("Connection lost or timeout for version " + currentVersion + ". Resetting for next attempt.");
                    connection = null;
                    stream = null;
                } catch (Exception e) {
                    log.severe("Unexpected error during fuzzing: " + e.getMessage());
                    return true;
                }
            }
        } catch (Exception e) {
            log.severe("Fatal diagnostic error: " + e.getMessage());
            return true;
        }

        return false;
    }
}