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
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.model.operation.*;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependencies;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.awt.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Dependencies({@Dependency(type = NormalAuthenticationSituation.class), @Dependency(type = NormalRequestSituation.class)})
final class RequestBeforeAuthenticatedSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(RequestBeforeAuthenticatedSituation.class).formatter(Main.FORMATTER);

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        @Nullable Connection connection = null;
        try (
                @NotNull LogCtx.Scope logContext1 = LogCtx.builder()
                        .put("compliance id", compliance)
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope1 = Stack.pushScope("Connection")
        ) {
            log.info("Creating new unauthenticated connection");
            connection = compliance.createConnection("non-authenticated", this);

            log.info("Creating new bidirectional stream");
            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();
            @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("request id", request.getMsgId())
                            .put("response id", request.getResponseId())
                            .put("request payload", request.getPayload())
                            .put("request timeout", request.getTimeout())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing Request operation");
                stream.writeByte((byte) 0x01);
                stream.write(request.toBytes());

                log.info("Waiting for server reaction");
                boolean disconnected = connection.awaitDisconnection(2, TimeUnit.SECONDS);
                if (disconnected) {
                    log.info("Connection was closed correctly by the server");
                    return false;
                }
                log.warn("Connection is still alive");

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Wait for Disapproved");
                    @NotNull Elapsed elapsed = new Elapsed();
                    @NotNull Result result = Result.readResult(stream, 5, TimeUnit.SECONDS);
                    elapsed.freeze();
                    log.info("The server took " + elapsed + " to build a Result");

                    if (result.isApproved()) {
                        log.severe("The server approved a non-existent authentication");
                        return true;
                    }

                    log.info("Disapproved result successfully received");

                    try {
                        stream.close();
                    } catch (IOException ignore) {

                    }

                    if (connection.isClosed()) {
                        log.info("Connection was closed correctly by the server after sending Disapproved");
                    }

                    log.warn("Server left connection open after sending Disapproved");

                    try {
                        connection.close();
                    } catch (IOException e) {
                        log.warn("Cannot close connection: " + e);
                    }
                } catch (IOException e) {
                    log.severe("Read Result fail: " + e.getMessage());
                    return true;
                }
            } catch (IOException e) {
                log.severe("Failed to write Request operation: " + e.getMessage());
                return true;
            }

            return false;
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
        } catch (ClassCastException e) {
            log.severe().cause(e).log("Internal error");
            return true;
        } catch (TimeoutException e) {
            log.severe("Result read timeout: " + e.getMessage());
            return true;
        }
    }
}