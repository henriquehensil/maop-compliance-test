package dev.hensil.maop.compliance.situation.authentication;

import com.jlogm.Logger;
import com.jlogm.utils.Coloured;

import dev.hensil.maop.compliance.core.BidirectionalStream;
import dev.hensil.maop.compliance.core.Connection;
import dev.hensil.maop.compliance.exception.ConnectionException;
import dev.hensil.maop.compliance.exception.DirectionalStreamException;
import dev.hensil.maop.compliance.model.*;
import dev.hensil.maop.compliance.model.authentication.Approved;
import dev.hensil.maop.compliance.model.authentication.Authentication;
import dev.hensil.maop.compliance.model.authentication.Disapproved;
import dev.hensil.maop.compliance.model.authentication.Result;
import dev.hensil.maop.compliance.situation.Situation;

import dev.meinicke.plugin.annotation.Dependency;
import dev.meinicke.plugin.annotation.Plugin;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Plugin
@Dependency(type = NormalAuthenticationSituation.class)
final class InvalidVersionAuthenticationSituation extends Situation {

    // Static initializers

    private final @NotNull Logger log = Logger.create(InvalidVersionAuthenticationSituation.class);

    // Objects

    @Override
    public boolean diagnostic() {
        log.info("Starting authentication with invalid version diagnostic");

        log.info("Loading authentication presets");
        @NotNull Authentication authentication = new Authentication(getCompliance().getPreset());

        @NotNull String @NotNull [] invalidVersions = new String[] {
                "1.0", "1", "1.2.3.4", "v1.2.3", "01.2.3", "1.02.3", "1.2.03", "00.1.1", "1.2.3-",
                "1.2.3-alpha.-beta", "1.2.3-alpha.", "1.2.3-.", "1.2.3-.-", "1.2.3-alpha..",
                "1.2.3+build..", "1.2.3-alpha.", "1.2.3-.alpha", "1.2.3-alpha_beta", "1.2.3-alpha 1",
                "1.2.3-alpha.01", "1.2.3-00", "1.2.3-01",
        };

        @NotNull Set<MAOPError> expectedErrors = new HashSet<>() {{
            this.add(MAOPError.INVALID_FORMAT);
            this.add(MAOPError.PROTOCOL_VIOLATION);
        }};

        @NotNull ByteBuffer buffer = authentication.toByteBuffer();
        int versionIndex = buffer.limit() - 4;

        log.info("Starting authentication with invalid version events");

        @Nullable BidirectionalStream stream = null;
        @Nullable Connection connection = null;
        @Nullable Duration retryAfter = null;

        for (int i = 0; i < invalidVersions.length; i++) {
            // Replace the correct version for invalid
            buffer.put(versionIndex, (byte) invalidVersions[i].length());
            buffer.put(versionIndex + 1, invalidVersions[i].getBytes(StandardCharsets.UTF_8));

            try {
                // Creating connection
                if (connection == null) {
                    connection = getCompliance().createConnection("authenticationInvalidVersion", this);
                    stream = connection.createBidirectionalStream();
                }

                if (retryAfter != null) {
                    log.info("Waiting the \"retry_after\" be exceeded");
                    LockSupport.parkNanos(retryAfter.toNanos() + 1);
                }

                log.info("Writing authentication");
                stream.write(buffer.array(), 0, buffer.limit());
                log.info("Authentication request with invalid version sent successfully");
                log.info("Waiting for authentication response");


                @NotNull Result result;
                try {
                    long currentMs = System.currentTimeMillis();

                    long expectedBytes = 1 + (16 + 1 + 1) + (2 + 4 + 2 + 1) + 1 + 1 + 1 + 1;
                    long availableBytes;
                    do {
                        availableBytes = connection.awaitReading(stream, 2, TimeUnit.SECONDS);
                    } while (availableBytes < expectedBytes);
                    result = Result.readResult(stream);

                    int totalTime = Math.toIntExact(System.currentTimeMillis() - currentMs);

                    // Show performance
                    long l = result.getLength();
                    if (totalTime > 500) {
                        log.warn("Authentication response read " + Coloured.of("slowly").color(Color.RED).print() + ":" + l + " bytes in " + totalTime + " ms");
                    } else if (totalTime > 100) {
                        log.warn("Authentication response read reasonable: " + l + " bytes in " + totalTime + " ms");
                    } else {
                        log.info("Authentication response successfully receive: " + l + " bytes in " + totalTime + " ms");
                    }
                } catch (Throwable e) {
                    log.severe("Read Result failed: " + e);
                    return true;
                }

                if (result instanceof Approved) {
                    log.severe("Authentication with invalid version accepted with APPROVED");
                    return true;
                }

                @NotNull Disapproved disapproved = (Disapproved) result;
                @Nullable MAOPError error = MAOPError.get(disapproved.getErrorCode());
                if (error == null) {
                    log.severe("Maop error code not found: " + disapproved.getErrorCode());
                    return true;
                }

                if (!expectedErrors.contains(error)) {
                    log.warn("Error code \"" + error + "\" not suitable for the situation");
                    log.info("Error codes that may be suitable: " + expectedErrors);
                }

                log.info("Successfully disapproved with reason: " + disapproved.getReason());

                @Nullable Duration duration = disapproved.getRetryAfter();
                boolean canRetry = duration != null && duration.compareTo(Duration.ofSeconds(3)) < 0;

                if (!canRetry) {
                    log.warn("Ignoring \"retry_after\" field" + (duration != null ? "( " + duration.toMillis() + ")" : " ") + "and closing connection");

                    try {
                        connection.close();
                    } catch (IOException e) {
                        log.warn("Cannot close connection: " + e);
                    }

                    connection = null;
                    stream = null;

                    continue;
                }

                log.info("Reusing connection for other authentication with invalid version events");
                retryAfter = duration;
            } catch (DirectionalStreamException e) {
                log.severe("Failed to create bidirectional authentication stream: " + e);

                if (!connection.isConnected()) {
                    log.warn("Connection was closed before authentication could start");
                }

                return true;
            } catch (ConnectionException e) {
                if (i < 2) {
                    log.severe("Failed to create authentication connection: " + e.getCause());
                    return true;
                }

                log.warn("It was only possible to test " + i + " possibilities of authentication with invalid version");
            } catch (IOException e) {
                log.severe("Write failed: " + e);

                if (!connection.isConnected()) {
                    log.severe("Connection closed while sending authentication request: " + e);
                }

                return true;
            }
        }

        return false;
    }
}