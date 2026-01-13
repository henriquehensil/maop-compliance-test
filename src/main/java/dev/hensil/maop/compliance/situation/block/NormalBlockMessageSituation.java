package dev.hensil.maop.compliance.situation.block;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.UnidirectionalOutputStream;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.model.operation.*;
import dev.hensil.maop.compliance.situation.Situation;
import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
final class NormalBlockMessageSituation extends Situation {

    private static final @NotNull Logger log = Logger.create(NormalBlockMessageSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting diagnostics...");

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

            byte @NotNull [] bytes = new byte[200];
            Arrays.fill(bytes, (byte) 0xAB);

            @NotNull Message message = new Message((short) 2, bytes.length, (byte) 0);
            @NotNull Block block = new Block(bytes);
            @NotNull BlockEnd blockEnd = new BlockEnd(bytes.length);

            log.info("Writing message operation");
            stream.write(message.toBytes());
            log.info("Successfully written");

            log.info("Waiting for Proceed as response");
            @NotNull Operation operation = connection.await(stream, 2000, TimeUnit.SECONDS);
            if (!(operation instanceof Proceed)) {
                log.severe("Should be a Proceed but was " + operation.getClass().getSimpleName());
                return true;
            }

            log.info("Proceed successfully received");

            log.info("Writing block operation");
            stream.write(block.toBytes());

            log.info("Successfully written");

            log.info("Writing block end operation");
            stream.write(blockEnd.toBytes());

            log.info("Successfully written");

            log.info("Waiting for Done as response");
            operation = connection.await(stream, 2000, TimeUnit.SECONDS);
            if (!(operation instanceof Done)) {
                log.severe("Should be a Done but was " + operation.getClass().getSimpleName());
                return true;
            }

            log.info("Successfully done received!");

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
