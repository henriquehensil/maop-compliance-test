package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;
import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;
import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.Main;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Block;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependency(type = NormalBlockRequestSituation.class)
final class BlockAloneBidirectionalStreamSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(BlockAloneBidirectionalStreamSituation.class).formatter(Main.FORMATTER);

    // Object

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
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
            @NotNull Block block = new Block("Teste".getBytes(StandardCharsets.UTF_8));

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("block payload", block.getPayload())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing standalone Block operation");
                stream.write(block.getCode());
                stream.write(block.getBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Fail signal");
                    @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
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
                        log.warn("Cannot close bidirectional stream: " + e.getMessage());
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
        } catch (InterruptedException e) {
            return false;
        } catch (IOException e) {
            log.severe("Write failed: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Waiting fail operation timeout: " + e.getMessage());
            return true;
        }
    }
}