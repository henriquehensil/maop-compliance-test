package dev.hensil.maop.compliance.situation.block.request;

import com.jlogm.Logger;
import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.OperationUtil;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.model.SuccessMessage;
import dev.hensil.maop.compliance.model.operation.*;
import dev.hensil.maop.compliance.situation.Situation;
import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
public class NormalBlockRequestSituation extends Situation {

    private static final @NotNull Logger log = Logger.create(NormalBlockRequestSituation.class);

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

            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();

            byte @NotNull [] bytes = new byte[200];
            Arrays.fill(bytes, (byte) 0xAB);

            @NotNull Request request = new Request((short) 2, SuccessMessage.MESSAGE_ID, bytes.length, (byte) 0, 1000);
            @NotNull Block block = new Block(bytes);
            @NotNull BlockEnd blockEnd = new BlockEnd(bytes.length);

            log.info("Writing Request operation");
            stream.write(request.toBytes());
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

            log.info("Waiting for Response operation on the same stream");

            {
                int expectedBytes = OperationUtil.RESPONSE.getHeaderLength() + 1;
                long ms = System.currentTimeMillis();
                long available;
                do {
                    available = connection.awaitReading(stream, 1, TimeUnit.SECONDS);
                } while (available < expectedBytes);

                ms = ms - System.currentTimeMillis();
                if (ms > 600) {
                    log.warn("Takes " + ms + " ms to receive Response on the same stream");
                }
            }

            byte code = stream.readByte();
            @Nullable OperationUtil util = OperationUtil.getByCode(code);
            if (util == null) {
                log.severe("Theres not operation with code: " + code);
                return true;
            }

            if (util != OperationUtil.RESPONSE) {
                log.severe("It was expected to receive \"Response\" code but it was " + code + " (" + util.getName() + ")");
                return true;
            }

            @NotNull Response response = (Response) OperationUtil.RESPONSE.read(stream);
            if (response.getPayload() <= 0) {
                log.severe("Was expected a Success Message but the Response comes with empty payload: (payload = 0)" );
                return true;
            }

            log.info("Successfully Response received with " + response.getPayload() + " of payload");

            log.info("Waiting for the Block operation to read success message");
            {
                int expectedBytes = OperationUtil.BLOCK.getHeaderLength() + 1;
                long ms = System.currentTimeMillis();
                long available;
                do {
                    available = connection.awaitReading(stream, 2, TimeUnit.SECONDS);
                } while (available < expectedBytes);

                ms = ms - System.currentTimeMillis();
                if (ms > 600) {
                    log.warn("Takes " + ms + " ms to receive Response on the same stream");
                }
            }

            code = stream.readByte();
            util = OperationUtil.getByCode(code);
            if (util == null) {
                log.severe("Theres not operation with code: " + code);
                return true;
            }

            if (util != OperationUtil.BLOCK) {
                log.severe("It was expected to receive \"Block\" code but it was " + code + " (" + util.getName() + ")");
                return true;
            }

            block = (Block) util.read(stream);
            log.info("Successfully Block received");

            if (block.getPayload() != response.getPayload()) {
                log.severe("mismatch block payload (" + block.getPayload() + ")");
                return true;
            }

            log.info("Waiting Block end operation");
            {
                int expectedBytes = OperationUtil.BLOCK.getHeaderLength() + 1;
                long ms = System.currentTimeMillis();
                long available;
                do {
                    available = connection.awaitReading(stream, 2, TimeUnit.SECONDS);
                } while (available < expectedBytes);

                ms = ms - System.currentTimeMillis();
                if (ms > 600) {
                    log.warn("Takes " + ms + " ms to receive Response on the same stream");
                }
            }

            code = stream.readByte();
            util = OperationUtil.getByCode(code);
            if (util == null) {
                log.severe("Theres not operation with code: " + code);
                return true;
            }

            if (util != OperationUtil.BLOCK_END) {
                log.severe("It was expected to receive \"Block end\" code but it was " + code + " (" + util.getName() + ")");
                return true;
            }

            @NotNull SuccessMessage message;
            try {
                message = SuccessMessage.parse(block.getBytes());
            } catch (Throwable e) {
                log.severe("Illegal success message: " + e.getMessage());
                return true;
            }

            log.info("Successfully receive SuccessMessage with " + message.getBody().length + " of payload");

            try {
                stream.close();
            } catch (IOException e) {
                log.warn("Failure to close bidirectional stream" + e);
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
            return true;
        } catch (TimeoutException e) {
            log.severe("Fail waiter timeout");
            return true;
        }
    }
}
