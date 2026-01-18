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
import dev.hensil.maop.compliance.model.SuccessMessage;
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
public class NormalBlockRequestSituation extends Situation {

    private static final @NotNull Logger log = Logger.create(NormalBlockRequestSituation.class).formatter(Main.FORMATTER);

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

            log.info("Creating bidirectional stream");
            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

            byte @NotNull [] bytes = new byte[200];
            Arrays.fill(bytes, (byte) 0xAB);

            short TestMessageId = (short) 1; // Test message
            short responseId = (short) 0; // Success message
            byte priority = (byte) 0;

            @NotNull Request request = new Request(TestMessageId, responseId, bytes.length, priority, 1000);
            @NotNull Block block = new Block(bytes);
            @NotNull BlockEnd blockEnd = new BlockEnd(bytes.length);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("request id", request.getMsgId())
                            .put("request response id", request.getMsgId())
                            .put("request payload", request.getPayload())
                            .put("block payload", block.getPayload())
                            .put("block end total length", blockEnd.getTotal())
                            .put("stream id", stream.getId())
                            .install();

                    @NotNull Stack.Scope scope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing Request operation");
                stream.writeByte((byte) 0X01); // Request code
                stream.write(request.toBytes());

                log.info("Waiting for Proceed signal");
                @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                if (!(operation instanceof Proceed)) {
                    log.severe("Expected Proceed operation but it was " + operation.getClass().getSimpleName());
                    return true;
                }

                log.info("Writing Block operation");
                stream.writeByte((byte) 0x05); // Block code
                stream.write(block.toBytes());

                log.info("Writing BlockEnd operation");
                stream.writeByte((byte) 0x06); // Block end code
                stream.write(blockEnd.toBytes());

                try (@NotNull Stack.Scope scope1 = Stack.pushScope("Read")) {
                    log.info("Waiting for Response operation with SuccessMessage");

                    @NotNull SuccessMessage message = SuccessMessage.readAfterRequest(stream);
                    log.info("Successfully received SuccessMessage (payload = " + message.getBody().length + ")");

                    try {
                        stream.close();
                    } catch (IOException e) {
                        log.warn("Failure to close bidirectional stream: " + e);
                    }
                } catch (TimeoutException e) {
                    log.severe("Timeout waiting for Success Message: " + e.getMessage());
                    return true;
                } catch (IOException e) {
                    log.severe("Read failed: " + e);
                    return true;
                }

                return false;
            }
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (ClassCastException e) {
            log.severe().cause(e).log("Internal operation manager error");
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost during stream creation");
                return true;
            }

            log.severe("Failed to create Bidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.severe("Write failed: " + e);
            return true;
        } catch (TimeoutException e) {
            log.severe("Timeout waiting for Proceed operation");
            return true;
        }
    }
}
