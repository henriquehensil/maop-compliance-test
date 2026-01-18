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
import dev.hensil.maop.compliance.model.operation.Response;
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
@Dependency(type = NormalAuthenticationSituation.class)
final class ResponseAloneSituation extends Situation {

    private final @NotNull Logger log = Logger.create(ResponseAloneSituation.class).formatter(Main.FORMATTER);

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

            log.info("Creating bidirectional stream");
            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

            int payload = 200;
            long execStart = System.currentTimeMillis();
            int execTime = 1000;
            @NotNull Response response = new Response(payload, execStart, execTime);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("response payload", response.getPayload())
                            .put("response start exec", response.getStart())
                            .put("response end exec", response.getEnd())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing standalone Response operation");
                stream.write((byte) 0x02); // Response code
                stream.write(response.toBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Fail signal");
                    @NotNull Operation operation = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                    if (!(operation instanceof Fail fail)) {
                        log.severe("Fail operation was expected but was " + operation.getClass().getSimpleName());
                        return true;
                    }

                    @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                        this.add(MAOPError.ORDER_VIOLATION);
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
        } catch (IOException e) {
            log.severe("Write failed: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Waiting fail operation timeout: " + e.getMessage());

            if (!connection.isClosed()) {
                log.warn("Connection still alive");
            }

            return true;
        }
    }
}
