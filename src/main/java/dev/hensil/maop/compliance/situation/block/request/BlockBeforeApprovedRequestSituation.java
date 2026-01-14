package dev.hensil.maop.compliance.situation.block.request;

import com.jlogm.Logger;
import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.SuccessMessage;
import dev.hensil.maop.compliance.model.operation.*;
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
final class BlockBeforeApprovedRequestSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(BlockBeforeApprovedRequestSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        try {
            @Nullable Connection connection = getCompliance().getConnection("authentication");
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

            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

            byte @NotNull [] bytes = new byte[230];
            @NotNull Request request = new Request((short) 2, SuccessMessage.MESSAGE_ID, bytes.length, (byte) 0, 1000);
            @NotNull Block block = new Block(bytes);

            log.info("Writing message operation");
            stream.write(request.toBytes());

            log.info("Writing block operation before Approved");
            stream.write(block.toBytes());

            log.info("Successfully written both operations");

            log.info("Waiting for fail response");
            @NotNull Operation operation = connection.await(stream, 2000, TimeUnit.SECONDS);
            if (!(operation instanceof Fail fail)) {
                log.severe("Should be a Fail but was " + operation.getClass().getSimpleName());
                return true;
            }

            @Nullable MAOPError error = MAOPError.get(fail.getError());
            if (error == null) {
                log.severe("Error code not found: " + fail.getError());
                return true;
            }

            @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
                this.add(MAOPError.ORDER_VIOLATION);
                this.add(MAOPError.PROTOCOL_VIOLATION);
            }};

            if (!expectedErrors.contains(error)) {
                log.warn("Error code \"" + error + "\" not suitable for the situation");
                log.info("Error codes that may be suitable: " + expectedErrors);
            }

            log.info("Successfully Fail received");
            log.info("Reason: " + fail.getReasonToString());

            try {
                stream.close();
            } catch (IOException e) {
                log.warn("Cannot close bidirectional stream: " + e.getMessage());
            }

            return false;
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (ClassCastException e) {
            log.severe().cause(e).log("Internal operation manager error");
            return true;
        } catch (IOException e) {
            log.severe("Write failed: " + e);
            return true;
        } catch (InterruptedException e) {
            log.warn("Operation fail waiter interrupted");
        } catch (TimeoutException e) {
            log.severe("Fail waiter timeout");
            return true;
        }

        return false;
    }
}