package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.Main;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.*;

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
@Dependency(type = NormalBlockMessageSituation.class)
final class BlockEndBeforeBlockMessageSituation extends Situation {

    private static final @NotNull Logger log = Logger.create(BlockEndBeforeBlockMessageSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics...");

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

            @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();
            int total = 200;
            @NotNull Message message = new Message((short) 2, total, (byte) 0);
            @NotNull BlockEnd blockEnd = new BlockEnd(total);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("message id", message.getMsgId())
                            .put("message payload", message.getPayload())
                            .put("block end total length", blockEnd.getTotal())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing Message operation");
                stream.writeByte(message.getCode());
                stream.write(message.toBytes());

                log.info("Waiting for Proceed signal");
                @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                if (!(operation instanceof Proceed)) {
                    log.severe("Proceed operation was expected but was " + operation.getClass().getSimpleName());
                    return true;
                }

                log.info("Writing block end operation before");
                stream.writeByte(blockEnd.getCode());
                stream.write(blockEnd.toBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Fail signal");
                    operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                    if (!(operation instanceof Fail fail)) {
                        log.severe("Fail operation was expected but was " + operation.getClass().getSimpleName());
                        return true;
                    }

                    @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                        this.add(MAOPError.PAYLOAD_LENGTH_MISMATCH);
                        this.add(MAOPError.PROTOCOL_VIOLATION);
                    }};

                    @Nullable MAOPError error = MAOPError.get(fail.getError());
                    if (error == null) {
                        log.severe("error code not found: " + fail.getError());
                        return true;
                    }

                    if (!expectedErrors.contains(error)) {
                        log.warn("Error code \"" + error + "\" is not standard for this violation");
                        log.info("Acceptable error codes for this situation: " + expectedErrors);
                    }

                    log.info("Successfully Fail operation received (reason = " + fail.getReasonToString() + ")");

                    try {
                        stream.close();
                    } catch (IOException e) {
                        log.warn("Cannot close unidirectional stream: " + e.getMessage());
                    }

                    return false;
                } catch (TimeoutException e) {
                    log.severe("Waiting fail operation timeout: " + e.getMessage());
                    return true;
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
            log.severe("Failed to create Unidirectional stream: " + e.getMessage());
            return true;
        } catch (InterruptedException e) {
            return false;
        } catch (IOException e) {
            log.severe("Write failed: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Waiting Proceed operation timeout: " + e.getMessage());
            return true;
        }
    }
}