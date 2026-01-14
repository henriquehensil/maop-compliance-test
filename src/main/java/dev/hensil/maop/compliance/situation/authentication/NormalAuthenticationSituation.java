package dev.hensil.maop.compliance.situation.authentication;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;
import dev.meinicke.plugin.annotation.Priority;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
@Priority(value = -2)
final class NormalAuthenticationSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalAuthenticationSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting authentication diagnostic");

        @NotNull Connection connection;

        try {
            connection = getCompliance().createConnection("authentication", this);
        } catch (IOException e) {
            log.severe("Failed to create authentication connection: " + e.getMessage());
            return true;
        }

        log.info("Loading authentication presets");
        @NotNull Authentication authentication = new Authentication(getCompliance().getPreset());

        log.debug("Serializing authentication payload");
        byte @NotNull [] data = authentication.toByteBuffer().array();

        @NotNull BidirectionalStream stream;
        try {
            stream = connection.createBidirectionalStream();
        } catch (DirectionalStreamException e) {
            log.severe("Failed to create bidirectional authentication stream: " + e);

            if (!connection.isConnected()) {
                log.warn("Connection was closed before authentication could start");
            }

            return true;
        }

        log.info("Sending authentication request");

        try {
            stream.write(data);
        } catch (IOException e) {
            if (connection.isConnected()) {
                log.warn("Write failed, retrying once");

                try {
                    stream.write(data);
                } catch (IOException ex) {
                    log.severe("Authentication write failed after retry: " + ex);
                    return true;
                }
            } else {
                log.severe("Connection closed while sending authentication request: " + e);
                return true;
            }
        }

        log.info("Authentication request sent successfully");

        log.debug("Closing authentication output stream");
        try {
            stream.closeOutput();
        } catch (IOException e) {
            log.warn("Failed to close authentication output stream: " + e);

            if (!connection.isConnected()) {
                log.severe("Connection closed unexpectedly after authentication request");
                return true;
            }
        }

        log.info("Waiting for authentication response");

        try {
            {
                long expectedBytes = 1 + (16 + 1 + 1) + (2 + 4 + 2 + 1) + 1 + 1 + 1 + 1;
                long availableBytes;
                do {
                    availableBytes = connection.awaitReading(stream, 2, TimeUnit.SECONDS);
                } while (availableBytes < expectedBytes);
            }

            @NotNull Result result = Result.readResult(stream);
            if (result instanceof Disapproved disapproved) {
                log.warn("Authentication rejected");
                log.warn("code: " + disapproved.getErrorCode());
                log.warn("Reason: " + disapproved.getReason());
                return true;
            }

            log.info("Successfully authenticated!");

            // Shutdown
            try {
                log.debug("Closing authentication input stream");
                stream.closeInput();
            } catch (IOException e) {
                log.warn("Failed to close authentication input stream: " + e);
                if (!connection.isConnected()) {
                    log.severe("Connection closed unexpectedly after authentication");
                    return true;
                }
            }

            // Finish
            return false;
        } catch (IOException e) {
            log.severe("A error occurs while read result response from the server: " + e.getMessage());
            return true;
        } catch (TimeoutException e) {
            log.severe("Authentication result timed out");
            return true;
        }
    }
}