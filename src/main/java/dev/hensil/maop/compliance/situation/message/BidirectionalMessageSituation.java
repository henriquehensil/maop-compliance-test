package dev.hensil.maop.compliance.situation.message;

import com.jlogm.Logger;
import dev.hensil.maop.compliance.BidirectionalStream;
import dev.hensil.maop.compliance.Connection;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.exception.GlobalOperationManagerException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Message;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.situation.Situation;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@Plugin
@Dependency(type = NormalMessageSituation.class)
final class BidirectionalMessageSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(BidirectionalStream.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting diagnostics");

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

            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();
            @NotNull Message message = new Message((short) 1, 0L, (byte) 0);

            log.debug("Managing message operation to wait Done response");
            connection.manage(stream, message);

            log.info("Writing message operation");
            stream.write(message.toBytes());

            log.info("Successfully written");
            log.info("Waiting for FAIL as response");

            @NotNull Operation operation = connection.await(message, 2000);
            if (!(operation instanceof Fail fail)) {
                log.severe("It was expected to receive \"Fail\" but instead it was " + operation.getClass().getSimpleName());
                return true;
            }

            @Nullable MAOPError error = MAOPError.get(fail.getError());
            if (error == null) {
                log.severe("Error code not found: " + fail.getError());
                return true;
            }

            if (fail.getStream() != stream.getId()) {
                log.severe("The fail stream id field (" + fail.getStream() + ") is different that stream id whose message was sent (" + stream.getId() + ")");
                return true;
            }

            @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                this.add(MAOPError.INVALID_HEADER);
                this.add(MAOPError.ILLEGAL_STREAM);
            }};

            if (!expectedErrors.contains(error)) {
                log.warn("Error code \"" + error + "\" not suitable for the situation");
                log.info("Error codes that may be suitable: " + expectedErrors);
            }

            log.info("Successfully FAIL received");

            try {
                stream.close();
            } catch (IOException e) {
                log.warn("Failure to close bidirectional stream" + e);
            }

            return false;
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost");
            }

            log.severe("Failed to create Bidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.trace("Failed to write message operation: " + e.getMessage());
            return true;
        } catch (GlobalOperationManagerException | TimeoutException e) {
            if (e instanceof GlobalOperationManagerException) {
                throw new AssertionError("Internal error", e);
            }

            log.severe("No response \"Fail\" received in the global pool");
            return true;
        } catch (InterruptedException e) {
            log.warn("Wait FAIL was interrupted");
        }

        return false;
    }
}