package dev.hensil.maop.compliance.situation.message;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.Connection;
import dev.hensil.maop.compliance.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.operation.Done;
import dev.hensil.maop.compliance.model.operation.Message;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

@Plugin
final class NormalMessageSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalMessageSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting normal message diagnostics");

        @Nullable Connection connection = getCompliance().getConnection("authentication");

        try {
            if (connection == null) {
                log.warn("Authenticated connection lost");
                connection = getCompliance().createConnection("authentication", this);

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
            @NotNull Message message = new Message((short) 1, 0L, (byte) 0);

            log.debug("Managing message operation to wait Done response");
            connection.manage(stream, message);

            log.info("Writing message operation");
            stream.write(message.toBytes());

            log.info("Successfully written");
            log.info("Waiting for done as response");

            @NotNull Operation operation = connection.await(message, 2000);
            if (!(operation instanceof Done done)) {
                log.severe("It was expected to receive \"Done\" but instead it was " + operation.getClass().getSimpleName());
                return true;
            }

            log.info("Done received");
            @NotNull Done.Entry @NotNull [] entries = done.getEntries();
            if (entries.length > 1) {
                log.warn("Apparently, the DONE entries have a higher number than they should");
                log.info("Entries: " + Arrays.toString(entries));
            }

            boolean success = false;
            for (@NotNull Done.Entry entry : entries) {
                if (entry.getStream() == stream.getId()) {
                    success = true;
                }
            }

            if (!success) {
                log.severe("The ID of the stream whose Message Operation (" + stream.getId() + ") was sent was not found in DONE");
                return true;
            }

            log.info("Successfully Done for Message Operation");

            try {
                stream.close();
            } catch (IOException e) {
                log.warn("Failure to close unidirectional stream" + e);
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
        } catch (GlobalOperationManagerException | TimeoutException e) {
            if (e instanceof GlobalOperationManagerException) {
                throw new AssertionError("Internal error", e);
            }

            log.severe("No response \"Done\" received in the global pool");
            return true;
        } catch (InterruptedException e) {
            log.warn("Wait done was interrupted");
        }

        return false;
    }
}