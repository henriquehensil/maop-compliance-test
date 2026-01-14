package dev.hensil.maop.compliance.situation.request;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.model.operation.Request;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
final class UnidirectionalRequestSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(UnidirectionalRequestSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting diagnostics");

        @Nullable Connection connection = getCompliance().getConnection("authentication");

        try {
            if (connection == null || !connection.isAuthenticated()) {
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
            @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);

            log.info("Writing Request operation");
            stream.write(request.toBytes());

            log.info("Successfully written");

            log.info("Waiting for FAIL as response");
            @NotNull Operation operation = connection.await(stream, 2000, TimeUnit.SECONDS);
            if (!(operation instanceof Fail fail)) {
                log.severe("It was expected to receive \"Fail\" but instead it was " + operation.getClass().getSimpleName());
                return true;
            }

            @Nullable MAOPError error = MAOPError.get(fail.getError());
            if (error == null) {
                log.severe("Error code not found: " + fail.getError());
                return true;
            }

            @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                this.add(MAOPError.ILLEGAL_STREAM);
                this.add(MAOPError.PROTOCOL_VIOLATION);
            }};

            if (!expectedErrors.contains(error)) {
                log.warn("Error code \"" + error + "\" not suitable for the situation");
                log.info("Error codes that may be suitable: " + expectedErrors);
            }

            log.info("Successfully Fail received");

            try {
                stream.close();
            } catch (IOException ignored) {
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
        } catch (ClassCastException | TimeoutException e) {
            if (e instanceof ClassCastException) {
                log.severe().cause(e).log("Internal operation manager error");
                return true;
            }

            log.severe("No response \"Fail\" received in the global pool");
            return true;
        } catch (InterruptedException e) {
            log.warn("Wait FAIL was interrupted");
        }

        return false;
    }
}