package dev.hensil.maop.compliance.situation.request;

import com.jlogm.Logger;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.core.OperationUtil;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;

import dev.hensil.maop.compliance.model.SuccessMessage;
import dev.hensil.maop.compliance.model.operation.Block;
import dev.hensil.maop.compliance.model.operation.Request;
import dev.hensil.maop.compliance.model.operation.Response;

import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Plugin
@Category("Situation")
final class NormalRequestSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalRequestSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting normal message diagnostics");

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

            @NotNull BidirectionalStream stream = connection.createBidirectionalStream();
            @NotNull Request request = new Request((short) 1, (short) 0, 0L, (byte) 0, 1000);

            log.info("Writing Request operation");
            stream.write(request.toBytes());

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

            @NotNull Block block = (Block) util.read(stream);
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
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost");
            }

            log.severe("Failed to create unidirectional stream: " + e.getMessage());
            return true;
        } catch (IOException e) {
            log.trace("Failed to write message operation: " + e.getMessage());
            return true;
        } catch (ClassCastException e) {
            log.severe().cause(e).log("Internal error");
            return true;
        } catch (TimeoutException e) {
            log.severe("Bidirectional stream read timeout");
            return true;
        }
    }
}