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
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Message;
import dev.hensil.maop.compliance.model.operation.Operation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependency(type = NormalMessageSituation.class)
final class MessageBidirectionalStreamSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(MessageBidirectionalStreamSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics");

        @Nullable Connection connection = compliance.getConnection("authentication");

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Connection")
        ) {
            if (connection == null || !connection.isAuthenticated()) {
                log.warn("Authenticated connection lost");
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

            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();
            @NotNull Message message = new Message((short) 1, 0L, (byte) 0);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("message id", message.getMsgId())
                            .put("message payload", message.getPayload())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing message operation");
                stream.writeByte(message.getCode());
                stream.write(message.toBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Fail signal");
                    @NotNull Elapsed elapsed = new Elapsed();
                    @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                    elapsed.freeze();
                    if (!(operation instanceof Fail fail)) {
                        log.severe("Fail operation was expected but instead received " + operation.getClass().getSimpleName());
                        return true;
                    }

                    log.info("The server took " + elapsed + " to send Done operation");

                    @Nullable MAOPError error = MAOPError.get(fail.getError());
                    if (error == null) {
                        log.severe("Unknown error code received: " + fail.getError());
                        return true;
                    }

                    @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                        this.add(MAOPError.ILLEGAL_STREAM);
                        this.add(MAOPError.PROTOCOL_VIOLATION);
                    }};

                    if (!expectedErrors.contains(error)) {
                        log.warn("Error code \"" + error + "\" is not standard for this violation");
                        log.info("Acceptable error codes for this situation: " + expectedErrors);
                    }

                    log.info("Successfully received Fail operation");

                    try {
                        stream.close();
                    } catch (IOException e) {
                        log.warn("Failure to close bidirectional stream" + e);
                    }

                    return false;
                }
            }
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost during stream creation");
                return true;
            }
            log.severe("Failed to create Bidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.trace("Failed to write message operation: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Timeout waiting for Fail operation");
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}