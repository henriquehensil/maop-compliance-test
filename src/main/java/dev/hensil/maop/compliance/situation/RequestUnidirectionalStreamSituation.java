package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.model.operation.Request;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class RequestUnidirectionalStreamSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(RequestUnidirectionalStreamSituation.class);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics: Illegal Request on Unidirectional Stream");

        @Nullable Connection connection = compliance.getConnection("authentication");

        try (
                @NotNull LogCtx.Scope logContext1 = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation", getName())
                        .install();

                @NotNull Stack.Scope logScope1 = Stack.pushScope("Connection")
        ) {
            if (connection == null || !connection.isAuthenticated()) {
                log.warn("Authenticated connection lost. Reconnecting...");
                connection = compliance.createConnection("authentication", this);

                log.trace("Authenticating");
                try {
                    connection.authenticate();
                    log.trace("Successfully authenticated");
                } catch (Throwable e) {
                    log.severe("Authentication failure: " + e);
                    return true;
                }
            }

            log.info("Creating illegal Unidirectional stream for Request");
            @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();
            @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("request id", request.getMsgId())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing Request operation (Violating protocol: Unidirectional)");
                stream.writeByte(request.getCode());
                stream.write(request.toBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Fail operation from server");

                    @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);

                    if (!(operation instanceof Fail fail)) {
                        log.severe("Expected Fail operation, but instead received: " + operation.getClass().getSimpleName());
                        return true;
                    }

                    @Nullable MAOPError error = MAOPError.get(fail.getError());
                    if (error == null) {
                        log.severe("Unknown error code received: " + fail.getError());
                        return true;
                    }

                    @NotNull Set<MAOPError> expectedErrors = Set.of(
                            MAOPError.ILLEGAL_STREAM,
                            MAOPError.PROTOCOL_VIOLATION
                    );

                    if (!expectedErrors.contains(error)) {
                        log.warn("Error code \"" + error + "\" is not standard for this violation");
                        log.info("Acceptable error codes for this situation: " + expectedErrors);
                    }

                    log.info("Successfully received Fail operation as expected");
                    return false;

                } catch (IOException e) {
                    log.severe("Read failure: " + e.getMessage());
                    return true;
                }

            } catch (IOException e) {
                log.severe("Failed to write illegal message operation: " + e.getMessage());
                return true;
            }
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost during stream creation");
                return true;
            }
            log.severe("Failed to create Unidirectional stream: " + e.getMessage());
            return true;
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            return false;
        } catch (TimeoutException e) {
            log.severe("Timeout waiting for Fail operation");
            return true;
        }
    }
}