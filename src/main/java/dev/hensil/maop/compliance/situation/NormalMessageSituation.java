package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.Elapsed;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.Main;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.operation.Done;
import dev.hensil.maop.compliance.model.operation.Message;
import dev.hensil.maop.compliance.model.operation.Operation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class NormalMessageSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalMessageSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        @Nullable Connection connection = null;
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Authentication")
        ) {
            log.info("Starting normal message diagnostics");

            connection = compliance.getConnection("authentication");
            if (connection == null || !connection.isAuthenticated()) {
                log.warn("Authenticated connection lost");

                log.info("Creating new connection");
                connection = compliance.createConnection("authentication", this);
                log.info("Successfully created connection");

                log.info("Authenticating");
                try {
                    connection.authenticate();
                    log.trace("Successfully authenticated");
                } catch (Throwable e) {
                    log.severe("Authentication failure: " + e);
                    return true;
                }
            }

            log.info("Creating unidirectional stream");
            @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();
            log.info("Unidirectional stream successfully created");

            @NotNull Message message = new Message((short) 1, 0L, (byte) 0);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("message payload", message.getPayload())
                            .put("message id", message.getMsgId())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {

                log.info("Writing message operation");
                stream.writeByte(message.getCode());
                stream.write(message.toBytes());
                log.info("Successfully written");

                log.info("Waiting for Done operation as response");
                @NotNull Elapsed elapsed = new Elapsed();
                @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                elapsed.freeze();

                if (!(operation instanceof Done done)) {
                    log.severe("A Done operation was expected but it was " + operation.getClass().getSimpleName());
                    return true;
                }

                log.info("The server took " + elapsed + " to send Done operation");

                log.info("Done operation Successfully received");
                @NotNull Done.Entry @NotNull [] entries = done.getEntries();
                if (entries.length > 1) {
                    log.warn("Done entries have a higher number than they should: " + entries.length);
                }

                log.info("Successfully ");

                try {
                    stream.close();
                } catch (IOException e) {
                    log.warn("Failure to close unidirectional stream" + e);
                }
            }

            return false;
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost");
            }

            log.severe("Failed to create unidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.trace("Failed to write message operation: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Done waiting timeout: " + e.getMessage());
            return true;
        } catch (InterruptedException e) {
            log.warn("Wait done was interrupted");
        }

        return false;
    }
}