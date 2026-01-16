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
import dev.hensil.maop.compliance.model.authentication.Authentication;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;


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

    private final @NotNull Logger log = Logger.create(IllegalStreamAuthenticationSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting diagnostics");

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Connection")
        ) {

            log.info("Creating connection");
            @NotNull Connection connection = compliance.createConnection("AuthenticationIllegalStream", this);

            log.info("Creating illegal unidirectional stream");
            @NotNull UnidirectionalOutputStream invalidStream = connection.createUnidirectionalStream();

            log.info("Loading authentication presets");
            @NotNull Authentication authentication = new Authentication(compliance.getPreset());
            @NotNull ByteBuffer buffer = authentication.toByteBuffer();

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("authentication type", authentication.getType())
                            .put("authentication metadata", authentication.getMetadata())
                            .put("authentication version", authentication.getVersion())
                            .put("authentication vendor", authentication.getVendor())
                            .put("authentication total length", buffer.limit())
                            .put("connection", connection.toString())
                            .put("stream id", invalidStream.getId())
                            .put("is bidirectional", false)
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write");
            ) {

                log.info("Writing authentication to a illegal unidirectional stream");
                invalidStream.write(buffer.array(), buffer.position(), buffer.limit());

                log.info("Waiting for server reaction");
                boolean closed = connection.awaitDisconnection(2, TimeUnit.SECONDS);
                if (closed) {
                    log.info("Server correctly disconnected");
                    return false;
                }

                log.warn("Connection is still alive after write authentication in illegal message");

                try {
                    connection.close();
                } catch (IOException e) {
                    if (connection.isConnected()) { // If still connected
                        log.warn("Failed to close connection: " + e);
                    }
                }

                return false;
            } catch (IOException e) {
                log.severe("Write failed: " + e);
                return true;
            }
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create unidirectional stream: " + e.getMessage());
            return true;
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        }
    }
}