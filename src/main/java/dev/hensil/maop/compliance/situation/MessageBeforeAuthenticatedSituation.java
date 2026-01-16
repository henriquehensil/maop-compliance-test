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
import dev.hensil.maop.compliance.model.operation.Message;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependencies;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Plugin
@Category("Situation")
@Dependencies({@Dependency(type = NormalAuthenticationSituation.class), @Dependency(type = NormalMessageSituation.class)})
final class MessageBeforeAuthenticatedSituation extends Situation {

    private final @NotNull Logger log = Logger.create(MessageBeforeAuthenticatedSituation.class).formatter(Main.FORMATTER);

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics: Message before authentication");

        @Nullable Connection connection = null;
        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Connection")
        ) {
            log.info("Creating unauthenticated connection");
            connection = compliance.createConnection("non-authenticated", this);

            log.info("Creating new Unidirectional stream");
            @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();
            @NotNull Message message = new Message((short) 2, 0L, (byte) 0);

            try (
                    @NotNull LogCtx.Scope writeContext = LogCtx.builder()
                            .put("stream id", stream.getId())
                            .put("message id", message.getMsgId())
                            .put("message payload", message.getPayload())
                            .put("message code", message.getCode())
                            .install();

                    @NotNull Stack.Scope writeScope = Stack.pushScope("Write")
            ) {
                log.info("Writing Message operation on illegal state");
                stream.write(message.toBytes());

                log.info("Waiting for server reaction (Expected: Disconnect)");
                boolean disconnected = connection.awaitDisconnection(2, TimeUnit.SECONDS);
                if (disconnected) {
                    log.info("Connection was correctly closed by the server");
                    return false;
                }

                log.warn("Connection is still alive after illegal message");
                return false;
            }

        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost prematurely");
            }

            log.severe("Failed to create Unidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.severe("I/O error during message write: " + e.getMessage());
            return true;
        }
    }
}