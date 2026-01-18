package dev.hensil.maop.compliance.situation;

import com.jlogm.Logger;

import com.jlogm.context.LogCtx;
import com.jlogm.context.Stack;

import dev.hensil.maop.compliance.Elapsed;
import dev.hensil.maop.compliance.core.*;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;

import dev.hensil.maop.compliance.model.SuccessMessage;
import dev.hensil.maop.compliance.model.operation.*;

import dev.meinicke.plugin.annotation.Category;
import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

@Plugin
@Category("Situation")
@Dependency(type = NormalAuthenticationSituation.class)
final class NormalRequestSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(NormalRequestSituation.class).formatter(Main.FORMATTER);

    // Objects

    @Override
    public boolean diagnostic(@NotNull Compliance compliance) {
        log.info("Starting normal message diagnostics");

        @Nullable Connection connection = compliance.getConnection("authentication");

        try (
                @NotNull LogCtx.Scope logContext = LogCtx.builder()
                        .put("compliance id", compliance.getId())
                        .put("situation name", getName())
                        .install();

                @NotNull Stack.Scope logScope = Stack.pushScope("Connection")
        ) {
            if (connection == null || !connection.isAuthenticated()) {
                log.warn("Authenticated connection lost");
                connection = compliance.createConnection("authentication", this);

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
            @NotNull Request request = new Request((short) 1, SuccessMessage.MESSAGE_ID, 0L, (byte) 0, 1000);

            try (
                    @NotNull LogCtx.Scope logContext2 = LogCtx.builder()
                            .put("connection", connection)
                            .put("stream id", stream.getId())
                            .put("request id", request.getMsgId())
                            .put("response id", request.getResponseId())
                            .put("request payload", request.getPayload())
                            .put("request timeout", request.getTimeout())
                            .install();

                    @NotNull Stack.Scope logScope2 = Stack.pushScope("Write")
            ) {
                log.info("Writing Request operation");
                stream.writeByte(request.getCode());
                stream.write(request.toBytes());

                try (@NotNull Stack.Scope logScope3 = Stack.pushScope("Read")) {
                    log.info("Waiting for Proceed operation");
                    @NotNull Operation proceed = connection.awaitOperation(stream, 2, TimeUnit.SECONDS);
                    if (!(proceed instanceof Proceed)) {
                        log.severe("Proceed was expected but it was " + proceed.getClass().getSimpleName());
                        return true;
                    }

                    int expectedBytes = OperationUtil.RESPONSE.getHeaderLength() + 1;

                    log.info("Waiting for Response operation");
                    @NotNull Elapsed elapsed = new Elapsed();
                    connection.awaitReading(expectedBytes, stream, 3, TimeUnit.SECONDS);
                    elapsed.freeze();

                    byte code = stream.readByte();
                    @Nullable OperationUtil util = OperationUtil.getByCode(code);
                    if (util == null) {
                        log.severe("There is not operation with code: " + code);
                        return true;
                    }

                    if (util != OperationUtil.RESPONSE) {
                        log.severe("A Response operation was expected but it was " + code + " (" + util.getName() + ")");
                        return true;
                    }

                    @NotNull Response response = (Response) OperationUtil.RESPONSE.read(stream);
                    log.info("Is took " + elapsed + " to receive receive an potential Response operation");

                    if (response.getPayload() <= 0) {
                        log.severe("The Response comes with empty payload: (payload = 0)" );
                        return true;
                    }

                    if (response.getPayload() > Short.MAX_VALUE) {
                        log.severe("Response payload is too long for a simple \"Success message\": " + response.getPayload());
                        return true;
                    }

                    log.info("Successfully Response received with " + response.getPayload() + " of payload");

                    try (
                            @NotNull LogCtx.Scope logContext3 = LogCtx.builder()
                                    .put("response payload", response.getPayload())
                                    .put("response start exec", response.getStart())
                                    .put("response end exec", response.getEnd())
                                    .install();
                    ) {
                        @NotNull ByteBuffer successMessageBuffer = ByteBuffer.allocate((short) response.getPayload());

                        expectedBytes = OperationUtil.BLOCK.getHeaderLength() + 2;
                        log.info("Waiting for Block operation");
                        elapsed = new Elapsed();

                        connection.awaitReading(expectedBytes, stream, 3, TimeUnit.SECONDS);

                        int blocks = 0;
                        while (successMessageBuffer.remaining() > 0) {
                            try {
                                byte code2 = stream.readByte();
                                @Nullable OperationUtil blockUtil = OperationUtil.getByCode(code2);
                                if (blockUtil == null) {
                                    log.severe("There is not operation with code: " + code2);
                                    return true;
                                }

                                if (blockUtil == OperationUtil.BLOCK_END) {
                                    log.severe("Block end was received before reading all payload data (remaining = " + successMessageBuffer.remaining() + ")");
                                    return true;
                                }

                                if (blockUtil != OperationUtil.BLOCK) {
                                    log.severe("A Block operation was expected but it was " + code2 + " (" + blockUtil.getName() + ")");
                                    return true;
                                }

                                int blockPayload = stream.readInt();
                                if (blockPayload > response.getPayload()) {
                                    log.severe("Block payload is greater than the declared response payload: (block payload = " + blockPayload + " & response payload = " + response.getPayload() + ")");
                                    return true;
                                }

                                int total = successMessageBuffer.remaining() + blockPayload;
                                if (total > response.getPayload()) {
                                    throw new IOException("The number of blocks operations exceed the declared response payload (response payload = " + response.getPayload() + " & total read = " + total + ")");
                                }

                                connection.awaitReading(blockPayload, stream, 2, TimeUnit.SECONDS);

                                byte @NotNull [] bytes = new byte[blockPayload];
                                stream.readFully(bytes);
                                successMessageBuffer.put(bytes);

                                blocks++;
                            } catch (BufferOverflowException e) {
                                throw new IOException("The number of blocks operations exceed the declared response payload (response payload = " + response.getPayload() + ")");
                            }
                        }

                        elapsed.freeze();
                        if (blocks > 1) {
                            log.warn("It was necessary to read " + blocks + " blocks for a total of " + response.getPayload() + " payload data");
                        }

                        log.info("Waiting for block end operation");
                        elapsed = new Elapsed();
                        expectedBytes = OperationUtil.BLOCK_END.getHeaderLength() + 1;
                        connection.awaitReading(expectedBytes, stream, 2, TimeUnit.SECONDS);
                        elapsed.freeze();

                        byte code2 = stream.readByte();
                        @Nullable OperationUtil blockEndUtil = OperationUtil.getByCode(code2);
                        if (blockEndUtil == null) {
                            log.severe("There is not operation with code: " + code2);
                            return true;
                        }

                        if (blockEndUtil != OperationUtil.BLOCK_END) {
                            log.severe("A Block end operation was expected but it was " + code2 + " (" + blockEndUtil.getName() + ")");
                            return true;
                        }

                        @NotNull BlockEnd blockEnd = (BlockEnd) blockEndUtil.read(stream);
                        if (blockEnd.getTotal() != response.getPayload()) {
                            throw new IOException("Block end total bytes mismatch (block end total bytes = " + blockEnd.getTotal() + " & response payload = " + response.getPayload() + ")");
                        }

                        log.info("The server takes " + elapsed + " to send block end");

                        try (@NotNull Stack.Scope scope = Stack.pushScope("Success message parse")) {
                            @NotNull SuccessMessage message = SuccessMessage.parse(successMessageBuffer.array());
                            log.info("Successfully receive SuccessMessage (content type = " + message.contentTypeToString() + ")");

                            try {
                                stream.close();
                            } catch (IOException e) {
                                log.warn("Failure to close bidirectional stream" + e);
                            }

                            return false;
                        } catch (Throwable e) {
                            log.severe("Illegal success message: " + e.getMessage());
                            return true;
                        }
                    }
                } catch (IOException e) {
                    log.severe("Read failed: " + e);
                    return true;
                }
            } catch (IOException e) {
                log.severe("Write failed: " + e);
                return true;
            }
        } catch (ConnectionException e) {
            log.severe("Failed to create connection: " + e.getMessage());
            return true;
        } catch (DirectionalStreamException e) {
            if (!connection.isConnected()) {
                log.severe("Connection lost");
            }

            log.severe("Failed to create bidirectional stream: " + e.getMessage());
            return true;
        } catch (ClassCastException e) {
            log.severe().cause(e).log("Internal error");
            return true;
        } catch (TimeoutException e) {
            log.severe("Read timeout: " + e.getMessage());
            return true;
        }
    }
}