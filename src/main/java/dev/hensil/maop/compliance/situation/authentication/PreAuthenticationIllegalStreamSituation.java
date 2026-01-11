package dev.hensil.maop.compliance.situation.authentication;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class PreAuthenticationIllegalStreamSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(PreAuthenticationIllegalStreamSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting diagnostics");

        try {
            @NotNull Connection connection = getCompliance().createConnection("PreAuthenticationIllegalStream", this);
            @NotNull UnidirectionalOutputStream invalidStream = connection.createUnidirectionalStream();

            // Wait
            LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
            if (!connection.isConnected()) {
                log.warn("The connection has been closed.");

                try {
                    connection.close();
                } catch (IOException ignore) {
                    // Ignored
                }

                return false;
            }

            // So write
            @NotNull Authentication authentication = new Authentication(getCompliance().getPreset());
            @NotNull ByteBuffer buffer = authentication.toByteBuffer();

            log.info("Writing authentication on a unidirectional stream");
            invalidStream.write(buffer.array(), buffer.position(), buffer.limit());
            log.info("Successfully written");

            // Wait
            LockSupport.parkNanos(Duration.ofSeconds(1).toNanos());
            if (connection.isConnected()) {
                log.warn("The connection wasn't been close");

                try {
                    connection.close();
                } catch (IOException e) {
                    if (connection.isConnected()) { // If still connected
                        log.severe("Failed to close connection: " + e);
                        return true;
                    }
                }
            }

            return false;
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create unidirectional stream: " + e);
            return true;
        } catch (IOException e) {
            log.severe("Failed to create authentication connection: " + e.getMessage());
            return true;
        }
    }
}