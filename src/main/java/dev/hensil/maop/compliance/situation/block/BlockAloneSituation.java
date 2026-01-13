package dev.hensil.maop.compliance.situation.block;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.model.MAOPError;
import dev.hensil.maop.compliance.model.operation.Block;
import dev.hensil.maop.compliance.model.operation.Fail;
import dev.hensil.maop.compliance.model.operation.Operation;
import dev.hensil.maop.compliance.situation.Situation;

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
@Dependency(type = NormalBlockMessageSituation.class)
final class BlockAloneSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(BlockAloneSituation.class);

    // Object

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

            @NotNull UnidirectionalOutputStream stream = connection.createUnidirectionalStream();

            log.info("Creating block operation");
            @NotNull Block block = new Block("Teste".getBytes(StandardCharsets.UTF_8));

            log.info("Writing alone block operation ");
            stream.write(block.toBytes());

            @NotNull Operation operation = connection.await(stream, 2000, TimeUnit.SECONDS);
            if (!(operation instanceof Fail fail)) {
                log.severe("Should be a Fail but was " + operation.getClass().getSimpleName());
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
                log.warn("Error code \"" + error + "\" not suitable for the situation");
                log.info("Error codes that may be suitable: " + expectedErrors);
            }

            log.info("Successfully fail received with reason: " + fail.getReasonToString());

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
            return true;
        } catch (TimeoutException e) {
            log.severe("Fail waiter timeout");
            return true;
        }
    }
}