package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;
import dev.hensil.maop.compliance.core.Compliance;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Authentication;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;


/*
* Ao tentar autenticar-se usando uma stream inválida
* Espera-se que o servidor feche imediatamente a conexão
*
* Este teste não falhará como SEVERE caso a conexão ainda estabelecida.
* De qualquer forma, é importante que o servidor não consuma dados inúteis
*
* Possíveis erros: Erro na criação de conexão, Erro na criação de stream
* */
@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class IllegalStreamAuthenticationSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(IllegalStreamAuthenticationSituation.class);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics");

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Authentication")
        ) {

            log.info("Creating connection");
            @NotNull Connection connection = compliance.createConnection("AuthenticationIllegalStream", this);
            log.info("Connection successfully created");

            log.info("Creating unidirectional stream");
            @NotNull UnidirectionalOutputStream invalidStream = connection.createUnidirectionalStream();
            log.info("unidirectional stream successfully created");

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection.toString())
                            .put("stream id", invalidStream.getId())
                            .put("is bidirectional", false)
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write");
            ) {
                log.info("Loading authentication presets");
                @NotNull Authentication authentication = new Authentication(compliance.getPreset());
                @NotNull ByteBuffer buffer = authentication.toByteBuffer();

                log.info("Writing authentication to a unidirectional stream");
                invalidStream.write(buffer.array(), buffer.position(), buffer.limit());
                log.info("Successfully written");

                try (
                        @NotNull LogCtx.Scope logContext3 = LogCtx.builder()
                                .put("authentication type", authentication.getType())
                                .put("authentication token", authentication.getToken())
                                .put("authentication metadata", authentication.getMetadata())
                                .put("authentication version", authentication.getVersion())
                                .put("authentication vendor", authentication.getVendor())
                                .put("authentication total length", buffer.limit())
                                .install();

                        @NotNull Stack.Scope logScope3 = Stack.pushScope("Verification");
                ) {
                    log.info("Waiting for some server behaviour");
                    boolean closed = connection.awaitDisconnection(2, TimeUnit.SECONDS);
                    if (closed) {
                        log.info("Connection was closed correctly");
                        return false;
                    }

                    log.warn("The connection wasn't been close");

                    try {
                        connection.close();
                    } catch (IOException e) {
                        if (connection.isConnected()) { // If still connected
                            log.warn("Failed to close connection: " + e);
                        }
                    }

                    return false;
                }
            }
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create unidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        }
    }
}