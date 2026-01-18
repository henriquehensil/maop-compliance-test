package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.operation.*;

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
final class NormalBlockMessageSituation extends Situation {

    private static final @NotNull Logger log = Logger.create(NormalBlockMessageSituation.class);

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

                @NotNull Stack.Scope scope = Stack.pushScope("Connection")
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

            byte @NotNull [] bytes = new byte[200];
            Arrays.fill(bytes, (byte) 0xAB);

            @NotNull Message message = new Message((short) 2, bytes.length, (byte) 0);
            @NotNull Block block = new Block(bytes);
            @NotNull BlockEnd blockEnd = new BlockEnd(bytes.length);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("message id", message.getMsgId())
                            .put("message payload", message.getPayload())
                            .put("block payload", block.getPayload())
                            .put("block end total length", blockEnd.getTotal())
                            .put("stream id", stream.getId())
                            .install();

                    @NotNull Stack.Scope scope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing message operation");
                stream.writeByte(message.getCode());
                stream.write(message.toBytes());

                log.info("Waiting for Proceed signal");
                @NotNull Operation operation = connection.awaitOperation(stream, 2000, TimeUnit.SECONDS);
                if (!(operation instanceof Proceed)) {
                    log.severe("Should be a Proceed but was " + operation.getClass().getSimpleName());
                    return true;
                }

                log.info("Writing Block operation");
                stream.writeByte(block.getCode());
                stream.write(block.toBytes());

                log.info("Writing BlockEnd operation");
                stream.writeByte(blockEnd.getCode());
                stream.write(blockEnd.toBytes());

                try (@NotNull Stack.Scope scope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Done signal");
                    operation = connection.awaitOperation(stream, 2000, TimeUnit.SECONDS);
                    if (!(operation instanceof Done)) {
                        log.severe("Should be a Done but was " + operation.getClass().getSimpleName());
                        return true;
                    }

                    log.info("Successfully received Done operation");

                    try {
                        stream.close();
                    } catch (IOException e) {
                        log.warn("Cannot close unidirectional stream: " + e.getMessage());
                    }

                    return false;
                } catch (TimeoutException e) {
                    log.severe("Done operation waiting timeout: " + e.getMessage());
                    return true;
                }
            } catch (TimeoutException e) {
                log.severe("Proceed operation waiting timeout: " + e.getMessage());
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
        } catch (IOException e) {
            log.severe("Write failed: " + e);
            return true;
        }
    }
}
